% test GxB_select
clear all
make

for k = 32 % [1 2 4 8 16 32]
    nthreads_set (k)
    debug_off
    grbinfo
    test27  % band, for user-defined
    debug_on
    grbinfo
    test27
end

