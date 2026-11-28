package schemaevolution

import (
	"context"
	"fmt"
	"io"
	"os/exec"
)

// DockerPostgresArchive uses the database container's matching pg_dump/pg_restore
// binaries. Container/database identities are supplied by the deployment driver;
// archive storage and recovery-cut custody remain outside that container.
type DockerPostgresArchive struct{ Container, Database, User string }

func (d DockerPostgresArchive) validate() error {
	if d.Container == "" || d.Database == "" || d.User == "" {
		return fmt.Errorf("postgres_archive_identity_required")
	}
	return nil
}
func (d DockerPostgresArchive) Dump(ctx context.Context, snapshot string, out io.Writer) error {
	if err := d.validate(); err != nil {
		return err
	}
	if snapshot == "" {
		return fmt.Errorf("postgres_export_snapshot_required")
	}
	command := exec.CommandContext(ctx, "docker", "exec", d.Container, "pg_dump", "--username="+d.User, "--dbname="+d.Database, "--format=custom", "--snapshot="+snapshot, "--no-owner", "--no-privileges")
	command.Stdout = out
	if err := command.Run(); err != nil {
		return fmt.Errorf("postgres_archive_dump_failed: %w", err)
	}
	return nil
}
func (d DockerPostgresArchive) Restore(ctx context.Context, in io.Reader) error {
	if err := d.validate(); err != nil {
		return err
	}
	command := exec.CommandContext(ctx, "docker", "exec", "-i", d.Container, "pg_restore", "--username="+d.User, "--dbname="+d.Database, "--exit-on-error", "--no-owner", "--no-privileges")
	command.Stdin = in
	if err := command.Run(); err != nil {
		return fmt.Errorf("postgres_archive_restore_failed: %w", err)
	}
	return nil
}
