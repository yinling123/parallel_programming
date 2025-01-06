#include "work.hpp"

void run(const char* path)
{  
    //检查路径
    hdfsFS fs = get_hdfs_fs();
    if(hdfsExists(fs, path))
    {
        std::cout << "路径不存在" << std::endl;
        exit(-1);
    }
    //分发文件名
    vector<vector<string> >* arrangement;
    if(_my_rank == 0)
    {
        //分割文件
        arrangement = dispather(path);
        master_scatter(*arrangement);
        //获取主进程处理的文件
        vector<string>& assignedSplits = (*arrangement)[0];
        //读取图数据
        for(int i = 0; i < assignedSplits.size(); i++)
        {
            load_graph(assignedSplits[i].c_str(), vertexs);
        }
        delete arrangement;
    }
    else
    {
        vector<string> assignedSplits;
        //其他进程接收节点
        slave_scatter(assignedSplits);
        //加载图数据
        for(int i = 0; i < assignedSplits.size(); i++)
        {
            load_graph(assignedSplits[i].c_str(), vertexs);
        }
    }

    //分配顶点
    sync_graph(vertexs);

    // for(int i = 0; i < vertexs.size(); i++)
    // {
    //     std::cout <<  vertexs[i]->ID << " " << vertexs[i]->adj.size() << std::endl;
    // }

    // std::cout << "顶点分配完毕" << std::endl;

    //所有进程将顶点信息写入到hdfs中
    write_to_hdfs(vertexs);

    //释放vertex
    for(int i = 0; i < vertexs.size(); i++)
    {
        delete vertexs[i];
    }
}

int main(int argc, char** argv)
{
    //初始化MPI环境
    init_MPI(&argc, &argv);
    //运行函数
    run(argv[1]);
    //设置同步点
    worker_barrier();
    //释放资源
    worker_finalize(); 
    return 0;
}
