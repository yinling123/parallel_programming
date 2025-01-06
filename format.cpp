#include<iostream>
#include<fstream>
#include<unordered_map>
#include <vector>

using namespace std;

int main()
{
    //行读取数据集
    ifstream in("/mnt/sevenT/yinboh/project/dataset/com-youtube.ungraph.txt", ios::in);
    ofstream out("/mnt/sevenT/yinboh/project/dataset/com-youtube.ungraph.txt.format", ios::out | ios::trunc);

     if (!in.is_open()) {
        cout << "无法打开输入文件" << endl;
        return 1;
    }

    //进行行读取
    int ID = -1, adj = -1;
    int now;
    int idx = 0;
    unordered_map<int, vector<int>> map;
    
    while(in >> ID >> adj)
    { 
        if(idx == 0)
        {
            now = ID;
            idx++;
        }
        if(ID == now)
        {
            map[now].push_back(adj);
        }
        else
        {
            //将这一行写入到文件中
            out << now << " ";
            for(auto i : map[now])
            {
                out << i << " ";
            }
            out << endl;
            map.clear();
            now = ID;
            map[ID].push_back(adj);
        }
    }

    out << ID << " ";
    for(auto i : map[ID])
    {
        out << i << " ";
    }
    out << endl;
    map.clear();
    now = ID;
    

    //关闭连接
    in.close();
    out.close();
}