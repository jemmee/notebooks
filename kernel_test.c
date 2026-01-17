// sudo dnf groupinstall "Development Tools" -y
// sudo dnf install kernel-devel-$(uname -r) kernel-headers-$(uname -r) -y
//
// make -f Makefile_kernel_test
//
// sudo insmod kernel_test.ko
//
// dmesg | tail
//
// sudo rmmod kernel_test

#include <linux/init.h>   // Needed for the macros
#include <linux/kernel.h> // Needed for KERN_INFO
#include <linux/module.h> // Needed by all modules

// Metadata about the module
MODULE_LICENSE("GPL");
MODULE_AUTHOR("Kin Man Yung");
MODULE_DESCRIPTION("A simple Hello World Kernel Module");

// This function runs when the module is loaded (insmod)
static int __init kernel_start(void) {
  printk(KERN_INFO "Hello! The module is now in the kernel.\n");
  return 0; // A non-zero return means the module failed to load
}

// This function runs when the module is removed (rmmod)
static void __exit kernel_end(void) {
  printk(KERN_INFO "Goodbye! The module has been removed.\n");
}

// Register the start and end functions
module_init(kernel_start);
module_exit(kernel_end);