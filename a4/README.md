CS 3240, Fall 2017, 
# A4: Parallel Sort (Cool Sort)

Because I don't want to get too adventurous (as I look at remaining weeks) I will <sigh> once again use this 
assignment to explore threads. I wish there were more synchronization issues but ... you're welcome. As we discuss this 
, you will be provided examples that demo how to move data to and from threads. Also, shall we assume that you will use 
the system sort routine instead of writing your own. This will be straightahead work, so don't dally and just get busy 
on this. 

[And NOW ... Rocco]

In this assignment you will be subjected to a 100% completely realistic scenario. Somebody borked our database and now 
the records are all chopped up and out of order. So now it's YOUR job to clean up this mess and write a program to sort 
these back into a single file. Oh, and we only have a few nanoseconds to do it, so you'd better do it with pthreads!

# The Assignment

## Input Data
As input, you will receive an arbitrary number of ASCII text files with arbitrary file names. These files will all be 
located in a directory specified on the command line. You are given a set of sample data with which to test your 
program.

Your program should obtain a list of the files in a given directory (via command line argument argv[1]) and then use 
that to go about its business. 

Each data file will have a number of records that look like the following: 

    <username>,<password><blood type>,<domain name>,<database index>

You may read these in with any file manipulation function that you see fit (`fgets()` works quite nicely). You should 
split these on commas and before sticking them into a struct. 

The end result will be an array of these structs to be sorted. 

## Parallelism
Each file should be handled in parallel. This means that you will list the supplied directory and then spin up a 
separate thread for each file contained therein. 

All file manipulation functions and sorting will take place in the thread allotted for that file. 

## Benchmarking
You will be benchmarking this particular assignment. You will use the gettimeofday() function to start a
timer at the beginning of your program, and use it again at the end of your program to report how long your
program takes to run.

## Sorting 
You can write your own sort if you'd like, but since that's not relevant to the assignment, you can go ahead and use 
the `qsort()` function in the standard library. You will be sorting by the last field in each record, the `database 
id`. The result should be in ascending order. 

Note that this function takes a function pointer as an argument. This is a pointer to another function in memory which 
is used to compare elements in the array. You will learn more about how this works in class. 

## Makefile
Your makefile should produce an executable which takes one argument (a directory path) and performs a sort on the 
files located in that directory. Make sure that you are processing this argument in a fashion that finds the directory 
relative to the current working directory. (e.g., if you execute `./coolsort "./nested/directory/structure"`, 
everything in the subdirectory "structure" should be sorted). 

## Output
The output should be a single ASCII file called `sorted.yay` containing all of the records from each individual data 
file in sorted order. You will compile your code with a makefile that you write and upload that contains 

all:
	'gcc -Wall -Wpedantic -std=c99 *.c -o coolSort -pthread'

Do not name your output anything other than 'sorted.yay'. Place it in the directory containing the records that were sorted.

Program must work on both 'small-data' and 'big-data', and any other possible number of files.

You will lose points if it does not work on both datasets and naming specifications are not followed.

## Other Things
I am not responsible for anything that you may find at any of the domain names in the test data. 

Good luck and have fun. 
