#ifndef __WORKER_HPP__
#define __WORKER_HPP__

#include<vector>
#include<stdio.h>
#include<mpi.h>
#include"../util/header/hdfs_util.hpp"
#include<iostream>
#include<string>
#include"../util/header/serialization.hpp"
#include<sstream>
#include<algorithm>

//记录当前进程数量和当前进程ID
extern int _num_workers;
extern int _my_rank;

struct file_info {
    char* name;
    tOffset size;

    //按照文件大小降序
    bool operator<(const file_info& rhs) const
    {
        return size > rhs.size;
    }

};

struct vertex_info 
{
    int ID;
    std::vector<int> adj;
};

extern std::vector<vertex_info*> vertexs;

void send_ibinstream(ibinstream& m, int dst, int tag);

ibinstream& operator<<(ibinstream& m, const vertex_info& v);

obinstream& operator>>(obinstream& m, vertex_info& v);

//将数据分发到各个进程
template<class T>
void master_scatter(std::vector<T>& to_send);

//获取分发的数据
template<class T>
void slave_scatter(T& to_get);

//加载图数据
void load_graph(const char* path, std::vector<vertex_info*>& files);

vertex_info* to_vertex(char* line);

//初始化MPI环境
void init_MPI(int * argc, char*** argv);

//MPI同步
void worker_barrier();

//清空MPI环境
void worker_finalize();

//获取当前进程ID
int get_worker_id();

//获取系统进程数量
int get_num_workers();

//将hdfs中分割的文件分配给进程
vector<vector<string>>* dispather(const char* path);

//获取哈希结果
int hash_result(int vid);

obinstream recv_obinstream(int src, int tag);

//进行全部分发
void all_to_all(vector<vector<vertex_info*>> &vertexs, int tag);

//进行同步hash分配图数据
void sync_graph(std::vector<vertex_info*>& vertexs);

void write_to_hdfs(std::vector<vertex_info*> vertexs);

#include "work.cpp"
#endif