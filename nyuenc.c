#include <stdio.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>

#define CHUNK_SIZE 4096


/*
@author
Alessandro Landi

Run-length Encoding with Thread Pools

Could be furher optimized using linked lists
Doesn't handle if there are more than 255 characters in a row

./nyuenc [-j #] input.txt

*/


typedef struct Task{
   int id;
   char* addr;
   size_t length;

}Task;


Task* taskQueue;
int taskCount = 0;
int taskHead = 0;
char** taskCompleted;
int* taskIndex;
pthread_mutex_t mutexQueue;
pthread_mutex_t mutexCompleted;
pthread_cond_t condCompleted;
pthread_cond_t condQueue;


void executeTask(Task *task){

    int id = task->id;
    size_t length = task->length;
    char* temp = task->addr;
    char* data = malloc(sizeof(char) * (length));       //linked list implementation optimizes memory allocation
    
    data[0] = temp[0];
    int count = 1;
    data[1] = (char) count;
    int index = 1;

    for(size_t j = 1; j <= length - 1; j++){
        if( (temp[j]) == (temp[j - 1])){
                count++;
                data[index] = (char)count;
                  
            } else{
                index += 2;
                count = 1;
                data[index - 1] = temp[j];
                data[index] = (char)count;
        }
    }

    taskCompleted[id] = data;                
    pthread_mutex_lock(&mutexCompleted);        //waits for task to complete before adding the last index 
    taskIndex[id] = index;              
    pthread_cond_signal(&condCompleted);
    pthread_mutex_unlock(&mutexCompleted);
  
}


void submitTask(Task task){
   pthread_mutex_lock(&mutexQueue);
   taskQueue[taskHead + taskCount] = task;      // ll: linked list could possibly optimize this? this is a faux implementation that keeps track of the head
   taskCount++;
   pthread_cond_signal(&condQueue);
   pthread_mutex_unlock(&mutexQueue);
}


void* startThread(){
    while(1){
        Task task;
        pthread_mutex_lock(&mutexQueue);

        while(taskCount == 0){
            pthread_cond_wait(&condQueue, &mutexQueue);
        }

        task = taskQueue[taskHead];         //ll: and this
        taskHead++;
        taskCount--;

        pthread_mutex_unlock(&mutexQueue);

        executeTask(&task);
    }
}



int main(int argc, char *const *argv){

    int thread_num = 1;
    int opt;
    int cond_cmd = 1;

    while ((opt = getopt(argc, argv, "j:")) != -1) {
        switch (opt) {
            case 'j':
                thread_num = atoi(optarg);
                cond_cmd = 3;
        }
    }


    int fd;
    struct stat sb;
    char* addr;
    
    pthread_t th[thread_num];

    pthread_mutex_init(&mutexQueue, NULL);
    pthread_cond_init(&condQueue, NULL);
    pthread_mutex_init(&mutexCompleted, NULL);
    pthread_cond_init(&condCompleted, NULL);


    for(int i = 0; i < thread_num; i++){
        pthread_create(&th[i], NULL, &startThread, NULL);
    }
      
    for(int i = cond_cmd; i < argc; i++){
            
        taskHead = 0;

        fd = open(argv[i], O_RDONLY);
    
        if (fd == -1){
            fprintf(stderr, "Error: invalid file\n");
            return 1;
        }

        if (fstat(fd, &sb) == -1){
            fprintf(stderr, "Error: invalid file\n");
            return 1;
        }

        addr = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);

        if (addr == MAP_FAILED){
            fprintf(stderr, "Error: invalid file\n");
            return 1;
        }

    
        int total_chunks = (sb.st_size + CHUNK_SIZE - 1) / CHUNK_SIZE;

        taskQueue = malloc(sizeof(Task) * total_chunks);
        taskCompleted = malloc(sizeof(char*) * total_chunks);
        taskIndex = malloc(sizeof(int) * total_chunks);
    
        for(int j = 0; j < total_chunks; j++){
            Task t = {
                .id = j,
                .addr = &addr[j * CHUNK_SIZE],
                .length = (j + 1) * CHUNK_SIZE <= (int)sb.st_size ? CHUNK_SIZE : sb.st_size % CHUNK_SIZE
            };
            submitTask(t);
        }

        close(fd);

        char lastBit[2];        //stores the last bit in the encoded chunk: 0 is the char, 1 is the count

        /*
        Main thread handles stiching the encoded chunks. Waits until the task is completed before writing. 
        Writes the encoded chunk minus the last bit. Handles some edge cases when the file is smaller than 
        the static chunk size. 
        */
    
        for(int k = 0; k < total_chunks; k++){
            pthread_mutex_lock(&mutexCompleted);
            while(taskIndex[k] == 0){
                pthread_cond_wait(&condCompleted, &mutexCompleted);
            }
            pthread_mutex_unlock(&mutexCompleted);

            
            if(lastBit[0] == taskCompleted[k][0]){

                int currentCount = (int)taskCompleted[k][1] + (int)lastBit[1];

                taskCompleted[k][1] = (char)currentCount;
                        
                write(STDOUT_FILENO, taskCompleted[k], taskIndex[k] - 1);

                lastBit[0] = taskCompleted[k][taskIndex[k] - 1];
                lastBit[1] = taskCompleted[k][taskIndex[k]];

                if((k+1) == total_chunks && i + 1 == argc){
                    write(STDOUT_FILENO, lastBit, sizeof(lastBit));
                }

            }else{
                if(k > 0 || i > cond_cmd){
                    write(STDOUT_FILENO, lastBit, sizeof(lastBit));
                }
    
                if(total_chunks == 1 && i + 1 == argc){
                    write(STDOUT_FILENO, taskCompleted[k], taskIndex[k] + 1);
                }else {
                    write(STDOUT_FILENO, taskCompleted[k], taskIndex[k] - 1);
                }

                lastBit[0] = taskCompleted[k][taskIndex[k] - 1];
                lastBit[1] = taskCompleted[k][taskIndex[k]];

            }
        }
    }
       
    exit(0);

}