// sudo dnf install -y epel-release
// sudo dnf install -y gcc make fuse-devel fuse-libs
//
// gcc -Wall fuse_test.c `pkg-config fuse --cflags --libs` -o fuse_test
//
// mkdir ~/fuse_test
// ./fuse_test ~/fuse_test
//
// ls -l ~/fuse_test
// cat ~/fuse_test/hello
// fusermount -u ~/fuse_test

#define FUSE_USE_VERSION 26 // or 29 — both safe on Rocky 9
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

static const char *hello_path = "/hello";

static const char *hello_content = "Hello from FUSE on Rocky Linux!\n"
                                   "        Time:  2026     \n"
                                   "  Location: San Jose    \n\n";

static int hello_getattr(const char *path, struct stat *stbuf) {
  memset(stbuf, 0, sizeof(struct stat));

  if (strcmp(path, "/") == 0) {
    stbuf->st_mode = S_IFDIR | 0755;
    stbuf->st_nlink = 2;
    return 0;
  }

  if (strcmp(path, hello_path) == 0) {
    stbuf->st_mode = S_IFREG | 0444;
    stbuf->st_nlink = 1;
    stbuf->st_size = strlen(hello_content);
    return 0;
  }

  return -ENOENT;
}

static int hello_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi) {
  (void)offset;
  (void)fi;

  if (strcmp(path, "/") != 0)
    return -ENOENT;

  filler(buf, ".", NULL, 0);
  filler(buf, "..", NULL, 0);
  filler(buf, hello_path + 1, NULL, 0); // adds "hello"

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
  if (offset >= (off_t)len)
    return 0;

  if (offset + size > len)
    size = len - offset;

  memcpy(buf, hello_content + offset, size);

  return (int)size;
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