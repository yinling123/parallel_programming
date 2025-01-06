#include "work.hpp"
using namespace std;

//定义
int _num_workers;
int _my_rank;
std::vector<vertex_info*> vertexs;

void load_graph(const char *path, std::vector<vertex_info*>& vec)
{
    hdfsFS fs = get_hdfs_fs();
    hdfsFile fs_handle = get_r_handle(path, fs);
    LineReader reader(fs, fs_handle);
    while(true)
    {   
        reader.read_line();
        if(!reader.eof())
        {
            vertex_info* v = to_vertex(reader.get_line());
            vec.push_back(v);
        }
        else
        {
            break;
        }
    }
    hdfsCloseFile(fs, fs_handle);
    hdfsDisconnect(fs);
}

vertex_info* to_vertex(char *line)
{
    vertex_info* v = new vertex_info;
    char *pch;
    pch = strtok(line, " \t");
    v -> ID = atoi(pch);
    while((pch = strtok(NULL, " \t")))
    {
        v -> adj.push_back(atoi(pch));
    }
    return v;
}

void init_MPI(int *argc, char ***argv)
{
    int provided;
    //设置通信等级
    MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);
    //如果不支持MPI_THREAD_MULTIPLE，则退出程序
    if(provided != MPI_THREAD_MULTIPLE)
    {
        printf("MPI_THREAD_MULTIPLE is not supported\n");
        exit(-1);
    }
    //设置节点数和本节点的编号
    MPI_Comm_size(MPI_COMM_WORLD, &_num_workers);
    MPI_Comm_rank(MPI_COMM_WORLD, &_my_rank);
}

void worker_barrier()
{
    MPI_Barrier(MPI_COMM_WORLD);
}

void worker_finalize()
{
    MPI_Finalize();
}

int get_worker_id()
{
    return _my_rank;
}

int get_num_workers()
{
    return _num_workers;
}


vector<vector<string>>* dispather(const char *path)
{
    //存储分配的文件名
    vector<vector<string>>* assignment = new vector<vector<string>>(_num_workers);
    vector<vector<string>>& assignment_ref = *assignment;
    //获取hdfs文件连接
    hdfsFS fs = get_hdfs_fs();
    //获取文件数量
    int numFiles;
    hdfsFileInfo *fileinfo = hdfsListDirectory(fs, path, &numFiles);
    if(fileinfo == NULL)
    {
        printf("Error: hdfsListDirectory failed\n");
        exit(-1);
    }
    //设置每个进程的数据量
    size_t* assigned = new size_t[_num_workers];
    //初始化
    for(int i = 0; i < _num_workers; i++)
    {
        assigned[i] = 0;
    }

    //存储文件名并且按照文件大小排序
    vector<file_info> file_list;
    int avg = 0;
    for(int i = 0; i < numFiles; i++)
    {   //如果当前文件是目录，则跳过
        if(fileinfo[i].mKind == kObjectKindDirectory)
        {
            continue;
        }
        //添加并且统计总大小
        file_list.push_back(file_info{fileinfo[i].mName, fileinfo[i].mSize});
        avg += fileinfo[i].mSize;
    }

    avg /= _num_workers;

    //按照文件大小降序排序
    sort(file_list.begin(), file_list.end());

    //先根据局部性分配文件
    vector<file_info>::iterator it;
    //存储未分配的文件
    vector<file_info>recycle;

    for(it = file_list.begin(); it != file_list.end(); it++)
    {
        recycle.push_back(*it);
    }

    //按照大小进行分配
    for(it = recycle.begin(); it != recycle.end(); it++)
    {
        int min = 0;
        size_t min_size = assigned[0];
        for(int i = 1; i < _num_workers; i++)
        {
            if(assigned[i] < min_size)
            {
                min = i;
                min_size = assigned[i];
            }
        }
        assignment_ref[min].push_back(it -> name);
        assigned[min] += it -> size;
    }
    delete[] assigned;
    hdfsFreeFileInfo(fileinfo, numFiles);
    hdfsDisconnect(fs);
    return assignment;
}

int hash_result(int vid)
{
    return vid % _num_workers;
}

template <class T>
void master_scatter(vector<T> &to_send)
{
    //记录每个worker的数据量
    int* sendcounts = new int[_num_workers];
    for(int i = 0; i < _num_workers; i++)
    {
        sendcounts[i] = 0;
    }
    int recvcount = 0;
    //存储各部分进程的偏移
    int* offsets = new int[_num_workers];

    //创建存储数据的缓冲区
    ibinstream buffers;

    int size = 0;
    
    //统计每个进程的数据量
    for(int i = 0; i < get_num_workers(); i++)
    {
        //不对主进程发送数据
        if(i == _my_rank)
        {
            continue;
        }
        else
        {
            //将数据插入到缓冲区
            buffers << to_send[i];
            sendcounts[i] = buffers.size() - size;
            size = buffers.size();
        }
    }

    //进行分发数据
    MPI_Scatter(sendcounts, 1, MPI_INT, &recvcount, 1, MPI_INT, 0, MPI_COMM_WORLD);

    //获取各进程的偏移
    for(int i = 0; i < _num_workers; i++)
    {
        offsets[i] = (i == 0 ? 0 : offsets[i - 1] + sendcounts[i - 1]);
    }

    //获取缓冲区的数据
    char* buffer = buffers.get_buf();
    //开辟接收缓冲区
    char* recvbuffer;

    //进行分发数据
    MPI_Scatterv(buffer, sendcounts, offsets, MPI_CHAR, recvbuffer, recvcount, MPI_CHAR, 0, MPI_COMM_WORLD);

    //删除辅助数组
    delete[] sendcounts;
    delete[] offsets;
};

obinstream recv_obinstream(int src, int tag)
{  
    //存储MPI的status
	MPI_Status status;
    //检查MPI的消息发送情况，但是不接受，存储状态
	MPI_Probe(src, tag, MPI_COMM_WORLD, &status);
	int size;
    //获取发送消息的大小
	MPI_Get_count(&status, MPI_CHAR, &size); // get size of the msg-batch (# of bytes)
    //动态开辟缓冲区
    char * buf = new char[size];
    //接受消息
	MPI_Recv(buf, size, MPI_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    //将缓冲区的数据进行写入到输出缓冲区中
    return obinstream(buf, size);
}


template<class T>
void slave_scatter(T& to_get)
{
    //数据指针
    int* sendcounts;
    //记录缓冲区大小
    int recvcount;
    //偏移量指针
    int* offsets;

    //接收数据
    MPI_Scatter(sendcounts, 1, MPI_INT, &recvcount, 1, MPI_INT, 0, MPI_COMM_WORLD);

    //发送缓冲区
    char* sendbuffer;
    //接收缓冲区
    char* recvbuffer = new char[recvcount];

    //按照进程大小接收数据
    MPI_Scatterv(sendbuffer, sendcounts, offsets, MPI_CHAR, recvbuffer, recvcount, MPI_CHAR, 0, MPI_COMM_WORLD);

    //将数据写出
    obinstream um(recvbuffer, recvcount);
    um >> to_get;
};

ibinstream& operator<<(ibinstream& m, const vertex_info& v)
{
    m << v.ID;
    m << v.adj;
    return m;
}

obinstream& operator>>(obinstream& m, vertex_info& v)
{
    m >> v.ID;
    m >> v.adj;
    return m;
}

void send_ibinstream(ibinstream& m, int dst, int tag)
{
    MPI_Send(m.get_buf(), m.size(), MPI_CHAR, dst, tag, MPI_COMM_WORLD);  
}

void all_to_all(vector<vector<vertex_info *>> &vertex_s, int tag)
{
    //进行全局通信
    for(int i = 0; i < _num_workers; i++)
    {
        int partner = (i - _my_rank + _num_workers) % _num_workers;
        if(partner != _my_rank)
        {   
            //对于序列号小于自身序列号的
            if(partner > _my_rank)
            {
                //序列化缓冲区
                ibinstream* ib = new ibinstream;
                *ib << vertex_s[partner];

                //删除对应指针的空间
                for(int j = 0; j < vertex_s[partner].size(); j++)
                {
                    delete vertex_s[partner][j];
                }
                vector<vertex_info*>().swap(vertex_s[partner]);
                send_ibinstream(*ib, partner, tag);
                delete ib;

                //接收传输数据
                obinstream um = recv_obinstream(partner, tag);
                um >> vertex_s[partner];
            }
            else
            {
                obinstream um = recv_obinstream(partner, tag);
                ibinstream * m = new ibinstream;
                *m << vertex_s[partner];
                //删除指针
                for(int k = 0; k < vertex_s[partner].size(); k++)
                {
                    delete vertex_s[partner][k];
                }
                vector<vertex_info*>().swap(vertex_s[partner]);
                send_ibinstream(*m, partner, tag);
                delete m;
                um >> vertex_s[partner];
            }
        }
    }
}

void sync_graph(std::vector<vertex_info *>& vertex_s)
{
    //存储对应进程的数据
    vector<vector<vertex_info*>> parts(_num_workers);
    for(int i = 0; i < vertex_s.size(); i++)
    {
        vertex_info* v = vertex_s[i];
        parts[hash_result(v->ID)].push_back(v);
    }
    
    //进行全部分发
    all_to_all(parts, 200);

    vertex_s.clear();

    for(int i = 0; i < _num_workers; i++)
    {
       vertex_s.insert(vertex_s.end(), parts[i].begin(), parts[i].end());
    }
    parts.clear();
}

void write_to_hdfs(std::vector<vertex_info *> vertexs)
{   
    std::cout << _my_rank << " " << vertexs.size() << std::endl;
    hdfsFS fs = get_hdfs_fs();
    string path =  "/outMPI/" + to_string(_my_rank) + ".txt";
    hdfsFile out_file = get_w_handle(path.c_str(), fs);
    //将vertexs的值进行写入对应的hdfs文件
    string line;
    for(int i = 0; i < vertexs.size(); i++)
    {
        vertex_info* v = vertexs[i];
        if(v)
        {
            line = to_string(v->ID) + " " + to_string(v->adj.size());
            for(int j = 0; j < v->adj.size(); j++)
            {
                line += " " + to_string(v->adj[j]);
            }
            line += "\n";
            std::cout << _my_rank << " 写入第" << i << "行" << endl;
            hdfsWrite(fs, out_file, line.c_str(), line.size());
            line.clear();
        }
    }
    hdfsCloseFile(fs, out_file);
    hdfsDisconnect(fs);
}