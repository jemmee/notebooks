/*
as -o arm_assembly_test.o arm_assembly_test.asm

ld arm_assembly_test.o -o arm_assembly_test

./arm_assembly_test
*/

.section .data
    msg:
        .ascii "Stay in this land for a while, and I will be with you and will bless you.\n"      /* 10 is the ASCII for newline (\n) */
    .set len, . - msg                  /* Calculate the length of the string */

.section .text
    .global _start                        /* Standard entry point for the linker */

_start:
    /* syscall: write(int fd, const void *buf, size_t count) */
    mov x0, 1           /* Argument 1: File descriptor (1 = stdout) */
    ldr x1, =msg        /* Argument 2: Pointer to message */
    mov x2, len         /* Argument 3: Message length */
    mov x8, 64          /* Syscall ID for 'write' on ARM64 */
    svc 0               /* Execute syscall */

    /* syscall: exit(int status) */
    mov x0, 0           /* Argument 1: Exit status 0 */
    mov x8, 93          /* Syscall ID for 'exit' on ARM64 */
    svc 0               /* Execute syscall */