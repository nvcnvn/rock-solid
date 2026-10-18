push   %rbp
push   %r15
push   %r14
push   %r13
push   %r12
push   %rbx
sub    $0x28,%rsp
mov    0x30(%rdi),%r8
cmp    $0x18,%r8
jb     19615 <micro_rust::parse+0xf5>
mov    0x8(%rdi),%r9
mov    0x10(%rdi),%rdx
xor    %r15d,%r15d
mov    %r8,%rax
nopw   0x0(%rax,%rax,1)
lea    0x18(%r15),%r12
cmp    $0xffffffffffffffe7,%r15
ja     19624 <micro_rust::parse+0x104>
cmp    %rdx,%r12
ja     19624 <micro_rust::parse+0x104>
mov    0x4(%r9,%r15,1),%r13d
lea    0x18(%r13),%r14
cmp    %r14,%rax
jb     19615 <micro_rust::parse+0xf5>
movzbl (%r9,%r15,1),%edi
mov    0x8(%r9,%r15,1),%rbp
mov    0x10(%r9,%r15,1),%r10d
mov    0x10(%rsi),%rbx
cmp    (%rsi),%rbx
je     195cc <micro_rust::parse+0xac>
mov    0x8(%rsi),%rax
mov    %rbx,%rcx
shl    $0x5,%rcx
mov    %rbp,(%rax,%rcx,1)
mov    %r12,0x8(%rax,%rcx,1)
mov    %r13,0x10(%rax,%rcx,1)
mov    %r10d,0x18(%rax,%rcx,1)
mov    %dil,0x1c(%rax,%rcx,1)
inc    %rbx
mov    %rbx,0x10(%rsi)
add    %r14,%r15
mov    %r8,%rax
sub    %r15,%rax
cmp    $0x17,%rax
ja     19550 <micro_rust::parse+0x30>
jmp    19615 <micro_rust::parse+0xf5>
mov    %dil,0x3(%rsp)
mov    %rsi,%rdi
mov    %rsi,0x20(%rsp)
mov    %r8,0x18(%rsp)
mov    %rdx,0x10(%rsp)
mov    %r9,0x8(%rsp)
mov    %r10d,0x4(%rsp)
call   1cdc0 <<alloc::raw_vec::RawVec<micro_rust::Frame>>::grow_one>
mov    0x4(%rsp),%r10d
movzbl 0x3(%rsp),%edi
mov    0x8(%rsp),%r9
mov    0x10(%rsp),%rdx
mov    0x18(%rsp),%r8
mov    0x20(%rsp),%rsi
jmp    19591 <micro_rust::parse+0x71>
add    $0x28,%rsp
pop    %rbx
pop    %r12
pop    %r13
pop    %r14
pop    %r15
pop    %rbp
ret
lea    0x42985(%rip),%rcx        # 5bfb0 <__frame_dummy_init_array_entry+0x2b0>
mov    %r15,%rdi
mov    %r12,%rsi
call   20790 <core::slice::index::slice_index_fail>
