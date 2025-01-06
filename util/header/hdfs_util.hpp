/*
	进行hdfs的相关操作
*/

#ifndef HDFS_UTIL_HPP
#define HDFS_UTIL_HPP

#include<hdfs.h>
#include<mpi.h>
#include<iostream>
#include <cstdlib>
#include<cstring>

//自定义hdfs的缓冲区大小
static const int HDFS_BUF_SIZE = 65535;
//默认的每行大小
static const int LINE_DEFAULT_SIZE = 4096;
//默认的分割的hdfs文件大小
static const int HDFS_BLOCK_SIZE = 8388608;

//获取hdfs连接
hdfsFS get_hdfs_fs();

//获取本地文件系统连接
hdfsFS get_local_fs();

//是否强制删除hdfs
int hdfs_delete(hdfsFS& fs, const char* outdir, int flag = 1);

//获取文件连接
hdfsFile get_r_handle(const char* path, hdfsFS fs);

hdfsFile get_w_handle(const char* path, hdfsFS fs);

hdfsFile get_rw_handle(const char* path, hdfsFS fs);

//行读取类，local_file数据读取到hdfs
class LineReader
{
public:

	//缓冲区指针
	char* line;
	//缓冲区使用长度
	int length;
	//缓冲区大小
	int size;

	//默认构造函数
	LineReader();

	//有参析构函数
	LineReader(hdfsFS& fs, hdfsFile& handle);

	//析构函数
	~LineReader();

	//进行缓冲区扩展
	void double_linebuf();

	//将数据添加到缓冲区中
	void line_append(const char* first, int num);

	//从hdfs中读取文件到缓冲区
	void fill();

	//判断是否正确
	bool eof();

	//数据全部添加到行中
	void append_line();

	//读取行
	void read_line();

	//获取行数据
	char* get_line();

private:
	//缓冲区
	char buf[HDFS_BUF_SIZE];
	//缓冲区位置指针
	size_t buf_pos;
	//缓冲区实际使用长度
	size_t buf_size;
	//文件系统权柄
	hdfsFS fs;
	//文件权柄
	hdfsFile handle;
	//文件结束标志
	bool file_end;
};

//文件写入
class LineWriter
{
public:
	//无参构造函数
	LineWriter();
	
	//有参构造函数
	LineWriter(const char* path, hdfsFS fs, int flag);

	//析构函数
	~LineWriter();

	//生成新的hdfs文件名
	void new_file_handle();

	//写入hdfs
	void write_line(char* line, int num);

private:
	//文件系统权柄
	hdfsFS fs;
	//hdfs文件连接
	hdfsFile handle;

	//文件路径指针
	const char* path;
	//标明当前是否存在多个机器
	int m_ID;
	//记录当前文件数量
	int next_part;
	//记录当前hdfs的文件夹大小
	int cur_size;
};


//local -> hdfs
void put(const char* local_path, const char* hdfspath);

//倒序寻找
const char* rfind(const char* str, char delim);

# include "../src/hdfs_util.cpp"
# endif