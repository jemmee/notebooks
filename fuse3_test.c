// sudo dnf install -y fuse3 fuse3-libs fuse3-devel
//
// gcc -Wall fuse3_test.c `pkg-config fuse3 --cflags --libs` -o fuse3_test
//
// mkdir ~/fuse3_test
// ./fuse3_test ~/fuse3_test
//
// ls -l ~/fuse3_test
// cat ~/fuse3_test/hello
// fusermount -u ~/fuse3_test

#define FUSE_USE_VERSION 31
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

static const char *hello_path = "/hello";

static const char *hello_content = "Hello from FUSE 3 on Rocky Linux!\n"
                                   "        Time:  2020     \n"
                                   "  Location: Redwood City    \n\n";

static int hello_getattr(const char *path, struct stat *stbuf,
                         struct fuse_file_info *fi) {
  (void)fi;
  memset(stbuf, 0, sizeof(struct stat));

  if (strcmp(path, "/") == 0) {
    stbuf->st_mode = S_IFDIR | 0755;
    stbuf->st_nlink = 2;
    return 0;
  }
  if (strcmp(path, hello_path) != 0)
    return -ENOENT;

  stbuf->st_mode = S_IFREG | 0444;
  stbuf->st_nlink = 1;
  stbuf->st_size = strlen(hello_content);
  return 0;
}

static int hello_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi,
                         enum fuse_readdir_flags flags) {
  (void)offset;
  (void)fi;
  (void)flags;

  if (strcmp(path, "/") != 0)
    return -ENOENT;

  filler(buf, ".", NULL, 0, 0);
  filler(buf, "..", NULL, 0, 0);
  filler(buf, hello_path + 1, NULL, 0, 0);

  return 0;
}

static int hello_open(const char *path, struct fuse_file_info *fi) {
  if (strcmp(path, hello_path) != 0)
    return -ENOENT;

  if ((fi->flags & O_ACCMODE) != O_RDONLY)
    return -EACCES;

  return 0;
}

static int hello_read(const char *path, char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi) {
  size_t len;
  (void)fi;

  if (strcmp(path, hello_path) != 0)
    return -ENOENT;

  len = strlen(hello_content);
  if (offset < 0 || (size_t)offset >= len)
    return 0;

  if (offset + size > len)
    size = len - offset;

  memcpy(buf, hello_content + offset, size);
  return size;
}

static struct fuse_operations hello_oper = {
    .getattr = hello_getattr,
    .readdir = hello_readdir,
    .open = hello_open,
    .read = hello_read,
};

int main(int argc, char *argv[]) {
  return fuse_main(argc, argv, &hello_oper, NULL);
}