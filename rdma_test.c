// gcc rdma_test.c -o rdma_test -libverbs
//
// ./rdma_test

#include <infiniband/verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
  struct ibv_device **dev_list;
  struct ibv_context *context;
  struct ibv_pd *pd;
  struct ibv_mr *mr;
  int num_devices;

  // 1. Get the list of RDMA devices (e.g., mlx5_0)
  dev_list = ibv_get_device_list(&num_devices);
  if (!dev_list) {
    perror("Failed to get RDMA devices list");
    return 1;
  }

  // 2. Open the first available device
  context = ibv_open_device(dev_list[0]);
  if (!context) {
    fprintf(stderr, "Couldn't open device %s\n",
            ibv_get_device_name(dev_list[0]));
    return 1;
  }
  printf("Device %s opened successfully.\n",
         ibv_get_device_name(context->device));

  // 3. Allocate a Protection Domain (PD)
  // Think of this as a private sandbox for your memory
  pd = ibv_alloc_pd(context);
  if (!pd) {
    fprintf(stderr, "Couldn't allocate Protection Domain\n");
    return 1;
  }

  // 4. Register a "Small Batch" of memory for Ohtani stats
  size_t buf_size = 1024;
  void *buf = malloc(buf_size);
  strcpy(buf, "Player: Shohei Ohtani | HR: 54 | Status: UNICORN");

  // Pin the memory: LOCAL_WRITE allows the NIC to write here
  // REMOTE_READ allows other machines to read this without the CPU knowing
  mr = ibv_reg_mr(pd, buf, buf_size,
                  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);

  if (!mr) {
    fprintf(stderr, "Couldn't register Memory Region (MR)\n");
    return 1;
  }

  printf("Memory Registered!\n");
  printf("  Addr: %p\n", mr->addr);
  printf("  R_Key (Remote Access Key): 0x%x\n", mr->rkey);
  printf("  L_Key (Local Access Key): 0x%x\n", mr->lkey);

  // Clean up (In a real app, this happens after the game season ends)
  ibv_dereg_mr(mr);
  free(buf);
  ibv_dealloc_pd(pd);
  ibv_close_device(context);
  ibv_free_device_list(dev_list);

  return 0;
}