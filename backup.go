package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

func BackupPostgresToS3(ctx context.Context) error {
	if GlobalMinio == nil {
		return fmt.Errorf("minio client is not initialized")
	}

	// Get DB config
	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASSWORD")
	if dbPass == "" {
		dbPass = os.Getenv("DB_PASS")
	}
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	if dbPort == "" {
		dbPort = "5432"
	}
	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		return fmt.Errorf("DB_NAME is required")
	}

	// Get S3 config
	s3Bucket := os.Getenv("S3_BUCKET")
	if s3Bucket == "" {
		return fmt.Errorf("S3_BUCKET is required")
	}
	s3KeyPrefix := strings.TrimSuffix(os.Getenv("S3_KEY_PREFIX"), "/")

	objectName := fmt.Sprintf("%s_%s.sql", dbName, time.Now().Format("20060102_150405"))
	pgDumpPath := strings.TrimSpace(os.Getenv("PG_DUMP_PATH"))
	cmdArgs := []string{
		"-h", dbHost,
		"-p", dbPort,
		"-U", dbUser,
		"-F", "p",
		"-b",
		"-v",
		dbName,
	}

	var cmd *exec.Cmd
	switch {
	case runtime.GOOS == "windows" && (pgDumpPath == "" || strings.HasPrefix(pgDumpPath, "wsl:")):
		linuxPath := strings.TrimPrefix(pgDumpPath, "wsl:")
		if linuxPath == "" {
			linuxPath = "pg_dump"
		}
		cmd = exec.CommandContext(ctx, "wsl", append([]string{linuxPath}, cmdArgs...)...)
	default:
		if pgDumpPath == "" {
			pgDumpPath = "pg_dump"
		}
		if info, err := os.Stat(pgDumpPath); err == nil && info.IsDir() {
			pgDumpPath = filepath.Join(pgDumpPath, "pg_dump")
		}
		if runtime.GOOS == "windows" && filepath.Ext(pgDumpPath) == "" {
			pgDumpPath += ".exe"
		}
		cmd = exec.CommandContext(ctx, pgDumpPath, cmdArgs...)
	}
	cmd.Env = append(os.Environ(), "PGPASSWORD="+dbPass)
	cmd.Stderr = os.Stderr

	pipeReader, pipeWriter := io.Pipe()
	cmd.Stdout = pipeWriter

	cmdErrCh := make(chan error, 1)
	go func() {
		err := cmd.Run()
		pipeWriter.CloseWithError(err)
		cmdErrCh <- err
	}()

	if s3KeyPrefix != "" {
		objectName = fmt.Sprintf("%s/%s", s3KeyPrefix, objectName)
	}

	if _, err := GlobalMinio.UploadFile(ctx, objectName, pipeReader, -1, "application/octet-stream"); err != nil {
		pipeReader.CloseWithError(err)
		<-cmdErrCh
		return fmt.Errorf("failed to upload to object storage: %w", err)
	}

	if cmdErr := <-cmdErrCh; cmdErr != nil {
		return fmt.Errorf("pg_dump failed: %w", cmdErr)
	}

	pipeReader.Close()

	return nil
}
