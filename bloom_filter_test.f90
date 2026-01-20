! gfortran -o bloom_filter_test bloom_filter_test.f90
!
! ./bloom_filter_test

program bloom_filter_test
    implicit none

    ! Parameters: m = bit array size, k = number of hashes
    integer, parameter :: m = 100
    integer, parameter :: k = 3
    logical :: bits(m)
    
    character(len=20) :: additions(6)
    character(len=20) :: tests(5)
    integer :: i, j, h_idx
    logical :: found

    ! Initialize bit array to false
    bits = .false.

    ! New Testament Items
    additions = [character(len=20) :: "Fisher-Net", "Alabaster-Jar", &
                 "Thorns-Crown", "Seamless-Robe", "Five-Loaves", "Two-Fish"]

    print *, "--- Fortran Bloom Filter: Seeding New Testament Items ---"
    
    ! Adding items
    do i = 1, 6
        print *, "Added: ", trim(additions(i))
        do j = 1, k
            h_idx = get_hash(trim(additions(i)), j, m)
            bits(h_idx) = .true.
        end do
    end do

    print *
    print *, "--- Testing Membership ---"
    
    tests = [character(len=20) :: "Fisher-Net", "Two-Fish", &
             "Golden-Calf", "Fisher-Boat", "Centurion-Spear"]

    do i = 1, 5
        found = .true.
        do j = 1, k
            h_idx = get_hash(trim(tests(i)), j, m)
            if (.not. bits(h_idx)) then
                found = .false.
                exit
            end if
        end do
        
        if (found) then
            print *, trim(tests(i)), ": MAYBE PRESENT"
        else
            print *, trim(tests(i)), ": DEFINITELY ABSENT"
        end if
    end do

contains

    ! Simple DJB2-style hash function
    integer function get_hash(str, seed, max_m)
        character(len=*), intent(in) :: str
        integer, intent(in) :: seed, max_m
        integer(8) :: h  ! Use 8-byte integer for intermediate math
        integer :: p

        h = 5381 + seed
        do p = 1, len(str)
            h = ((h * 33) + ichar(str(p:p)))
        end do
        
        ! Map to array index 1 to max_m
        get_hash = mod(abs(h), int(max_m, 8)) + 1
    end function get_hash

end program bloom_filter_test