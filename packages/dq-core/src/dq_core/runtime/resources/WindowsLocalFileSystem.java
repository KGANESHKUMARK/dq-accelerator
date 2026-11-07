package dqaccelerator.hadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * LOCAL WINDOWS DEVELOPMENT HARNESS ONLY - never used on Databricks or any cluster.
 *
 * <p>Hadoop's RawLocalFileSystem shells out to winutils.exe on Windows to set and read POSIX
 * permissions. This POC avoids downloading third-party native binaries: permission/ownership
 * changes are ignored (NTFS ACLs still apply) and file statuses report Hadoop default
 * permissions instead of invoking winutils. All data reads/writes use the standard Hadoop
 * local file system implementation.
 */
public class WindowsLocalFileSystem extends LocalFileSystem {

  public WindowsLocalFileSystem() {
    super(new WindowsRawLocalFileSystem());
  }

  /** Raw local file system without winutils-dependent permission handling. */
  public static class WindowsRawLocalFileSystem extends RawLocalFileSystem {

    @Override
    public void setPermission(Path p, FsPermission permission) {
      // Intentionally a no-op for the local Windows harness.
    }

    @Override
    public void setOwner(Path p, String username, String groupname) {
      // Intentionally a no-op for the local Windows harness.
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      return withDefaultPermissions(super.getFileStatus(f));
    }

    @Override
    public FileStatus getFileLinkStatus(Path f) throws IOException {
      return withDefaultPermissions(super.getFileLinkStatus(f));
    }

    /**
     * Lists a directory with java.io only. The parent implementation calls FileUtil.list(), which
     * performs a native NativeIO.Windows access check that requires hadoop.dll.
     */
    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
      File local = pathToFile(f);
      if (!local.exists()) {
        throw new FileNotFoundException("File " + f + " does not exist");
      }
      if (!local.isDirectory()) {
        return new FileStatus[] {getFileStatus(f)};
      }
      String[] names = local.list();
      if (names == null) {
        throw new IOException("Invalid directory or I/O error occurred for dir: " + local);
      }
      List<FileStatus> results = new ArrayList<>(names.length);
      for (String name : names) {
        try {
          results.add(getFileStatus(new Path(f, new Path(null, null, name))));
        } catch (FileNotFoundException e) {
          // The directory listing changed while it was being read.
        }
      }
      return results.toArray(new FileStatus[0]);
    }

    @Override
    public boolean delete(Path p, boolean recursive) throws IOException {
      File local = pathToFile(p);
      if (!local.exists()) {
        return false;
      }
      if (local.isFile()) {
        return local.delete();
      }
      String[] children = local.list();
      if (!recursive && children != null && children.length != 0) {
        throw new IOException("Directory " + local + " is not empty");
      }
      return FileUtil.fullyDelete(local);
    }

    private static FileStatus withDefaultPermissions(FileStatus status) {
      if (status == null) {
        return null;
      }
      FsPermission permission =
          status.isDirectory() ? FsPermission.getDirDefault() : FsPermission.getFileDefault();
      String user = System.getProperty("user.name", "dq");
      return new FileStatus(
          status.getLen(),
          status.isDirectory(),
          status.getReplication(),
          status.getBlockSize(),
          status.getModificationTime(),
          status.getAccessTime(),
          permission,
          user,
          user,
          status.getPath());
    }
  }
}
