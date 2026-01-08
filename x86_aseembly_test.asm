# as -o x86_assembly_test.o x86_assembly_test.asm
#
# ld x86_assembly_test.o -o x86_assembly_test
#
# ./x86_assembly_test

# --- x86_assembly_test.asm (x86-64 Linux) ---
.section .data
    msg:
        .ascii "Stay in this land, and I will be with you and bless you.\n"
    
    .set len, . - msg

.section .text
    .global _start

_start:
    # syscall: write(1, msg, len)
    movq $1, %rax       # System call 1 is 'write' on x86-64
    movq $1, %rdi       # Argument 1: File descriptor (1 = stdout)
    movq $msg, %rsi     # Argument 2: Pointer to message string
    movq $len, %rdx     # Argument 3: Length of string
    syscall             # Execute syscall

    # syscall: exit(0)
    movq $60, %rax      # System call 60 is 'exit' on x86-64
    movq $0, %rdi       # Argument 1: Exit status 0
    syscall             # Execute syscall