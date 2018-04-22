#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <dirent.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>


//this is the struct that the data will be read into and sorted with
typedef struct _member_t
{
    char *username;
    char *password;
    char *bloodType;
    char *domainName;
    int databaseIndex;

}member_t;

//This is used so the program can pass how many items are in each array of member_ts
typedef struct _threadreturn_t
{
    member_t** memberarr;
    int numofmembers;

}threadreturn_t;

//predefining the write to file method
void writetofile(threadreturn_t **members, int *intarr, int total, int*bucketsfromempty, int numfiles);

//used with qsort to sort the arrays in each thread
int sortfiles(const void *file1, const void *file2)
{
    member_t **one = (member_t **)file1;
    member_t **two = (member_t **)file2;
    int firstindex = (*one)->databaseIndex;  /*get first index*/
    int secondindex = (*two)->databaseIndex; /*get second index*/

    return firstindex-secondindex; /*return positive or negative*/
}
//predefining the method for the threads
void *threadSort(void *);

int main(int argc, char **argv)
{
    DIR *opendirectory; //open the directory supplied as a parameter
    opendirectory = opendir(argv[1]);
    struct dirent *entry;
    int numfiles = 0;

    //count the number of files in the program
    while ((entry = readdir(opendirectory)) != NULL)
    {
        if (entry->d_type == DT_REG)
        {
            numfiles++;
        }
    }
    //start from the beginning, rewind wasn't working here
    closedir(opendirectory);
    opendirectory = opendir(argv[1]);

    pthread_t thread[numfiles]; //makes numfiles threads
    int fileread=0;
    for (int i=0; i<numfiles+2; i++){       //add 2 to deal with . and ..
        entry = readdir(opendirectory);
        //if the read entry is a file then append the name to path and send it with a thread
         if (entry->d_type == DT_REG)
        {
            char *cwd = malloc(256);
            strcat(cwd, argv[1]);
            strcat(cwd, "/");
            strcat(cwd, entry->d_name);
            pthread_create(&thread[fileread], NULL, threadSort, cwd);
            fileread++;
        }
    }
    
    int totalCount = 0; //total number of items in all threads
    int bucketarr[numfiles]; //full buckets number
    int bucketarrfromempty[numfiles]; //empty buckets which will count up till full
    
    threadreturn_t ** members = malloc(sizeof(threadreturn_t**)*numfiles); //an array of threadreturn_t structs which have an array of members and an int with how many
    for(int i = 0; i<numfiles; i++){
        void *returned;
        members[i]= malloc(sizeof(member_t**));
        pthread_join(thread[i], &returned);
       
        members[i]=returned; // add to members array
        totalCount+= members[i]->numofmembers; 
        bucketarr[i]= members[i]->numofmembers;
        bucketarrfromempty[i]=0;
    }
    writetofile(members, bucketarr, totalCount, bucketarrfromempty, numfiles);//send members array, full buckets, total count of all files, empty buckets, and number of files to be printed

}

void writetofile(threadreturn_t **members, int *intarr, int total, int*bucketsfromempty, int numfiles)
{
   
    int bucketwithmin=0; //to keep track of which bucket had the smallest database index
    int minDBindex = INT_MAX; //keep track of smallest index on that read
    //all variables from a member_t
    char *indextostr = malloc(50);
    char *uname = malloc(50);
    char *pword = malloc(50);
    char *blood = malloc(50);
    char *domain = malloc(50);

    //set up the file print
    FILE *fp;
    fp = fopen("sorted.yay", "w+");
    


    //read through once for each member in all datasets. for each bucket j check if all elements from the bucket have been read and if not then read the
    //buckets from empty jth element which is how far down the array you've worked so far by taking members out, or in our case just reading them once
    //each time save all member info and if you run across a member with a lower id replace it with that info. at the end form a string, write it to the file
    //increment the buckets from empty number for that particular bucket and after all loops close file


    char *string = malloc(512);
    for (int i = 0; i < total; i++)
    {
        minDBindex = INT_MAX;
        for(int j = 0; j<numfiles; j++){
            if(bucketsfromempty[j]<intarr[j]){
                if (members[j]->memberarr[bucketsfromempty[j]]->databaseIndex < minDBindex)
                {
                   
                        minDBindex = members[j]->memberarr[bucketsfromempty[j]]->databaseIndex;
                        uname = members[j]->memberarr[bucketsfromempty[j]]->username;
                        pword = members[j]->memberarr[bucketsfromempty[j]]->password;
                        blood = members[j]->memberarr[bucketsfromempty[j]]->bloodType;
                        domain = members[j]->memberarr[bucketsfromempty[j]]->domainName;
                        bucketwithmin = j;
                }
            }
        }
        bucketsfromempty[bucketwithmin]++;
        strcat(string, uname);
        strcat(string, ",");
        strcat(string, pword);
        strcat(string, ",");
        strcat(string, blood);
        strcat(string, ",");
        strcat(string, domain);
        strcat(string, ",");
        sprintf(indextostr, "%d", minDBindex);
        strcat(string, indextostr);
        strcat(string, "\n");
        fputs(string, fp);
        memset(string,0, 512);
    }
    fclose(fp);
}

void *threadSort(void *data)
{
    
    FILE *textfile; /* initialize a FILE object called csvfile */
    char *file = (char *)data;
    textfile = fopen(file, "r");
    char *line = malloc(256);
    int numofelements=0;
    while(fgets(line, 256, textfile)){ //read once to get number of elements in the file
            numofelements++;
    }
    rewind(textfile); //restart file to the front
    member_t **arr = malloc(numofelements * sizeof(member_t));
    int segmentnumber = 0;
    int length = 50;
    char * segment = malloc(100);
    char *newline = malloc(256);
    
    for(int i =0; i<numofelements; i++){
        fgets(line, 256, textfile);
        
        arr[i]= malloc(sizeof(member_t));
        newline = line;
        //fill all variables in the struct
        while (segmentnumber < 5)
        {
            segment = strsep(&newline, ","); /*splits the line at a demlimiter*/
            if (segmentnumber == 0)
            {
                arr[i]->username = malloc(length);
                strncpy(arr[i]->username, segment, length);
            }
            else if (segmentnumber == 1)
            {
                arr[i]->password = malloc(length);
                strncpy(arr[i]->password, segment, length);
            }
            else if (segmentnumber == 2)
            {
                arr[i]->bloodType = malloc(length);
                strncpy(arr[i]->bloodType, segment, length);
            }
            else if (segmentnumber == 3)
            {
                arr[i]->domainName = malloc(length);
                strncpy(arr[i]->domainName, segment, length);
            }
            else if (segmentnumber == 4)
            {
                arr[i]->databaseIndex = atoi(segment);
            }

            segmentnumber++;
    }
    segmentnumber = 0;
    }
    //sort the array
    qsort(arr, numofelements, sizeof(member_t *), sortfiles);
    //create the thread return and add the array as well as the int saying how many items are in it.
    threadreturn_t *toreturn = malloc(sizeof(toreturn));
    toreturn->memberarr = arr;
    toreturn->numofmembers = numofelements;
    
    return toreturn;
}