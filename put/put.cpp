#include "../util/header/hdfs_util.hpp"

int main(int argc, char* argv[])
{
    //获取输入的本地文件路径和hdfs文件目录
    const char* local_path = argv[1];
    const char* hdfs_path = argv[2];
    printf("local_path:%s\n", local_path);
    printf("hdfs_path:%s\n", hdfs_path);
    put(local_path, hdfs_path);
    return 0;
}