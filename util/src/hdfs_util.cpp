#include "../header/hdfs_util.hpp"

hdfsFS get_hdfs_fs()
{
    //获取hdfs连接
    hdfsFS fs = hdfsConnect("localhost", 9010);
    //如果连接为空，则连接失败，并退出
    if(!fs)
    {
        std::cout << "connect to hdfs failed" << std::endl;
        exit(-1);
    }
    return fs;
}

hdfsFS get_local_fs()
{   
    hdfsFS hs = hdfsConnect(NULL, 0);
    return hs;
}

int hdfs_delete(hdfsFS &fs, const char *outdir, int flag)
{
    if(flag)
    {
        hdfsDelete(fs, outdir, 1);
    }
    else
    {
        hdfsDelete(fs, outdir, 0);
    }
    return 0;
}

hdfsFile get_r_handle(const char *path, hdfsFS fs)
{
    hdfsFile handle = hdfsOpenFile(fs, path, O_RDONLY | O_CREAT, 0, 0, 0);
    if(!handle)
    {
        std::cout << "open file failed" << std::endl;
        exit(-1);
    }
    return handle;
}

hdfsFile get_w_handle(const char *path, hdfsFS fs)
{
    hdfsFile handle = hdfsOpenFile(fs, path, O_WRONLY | O_CREAT, 0, 0, 0);
    if(!handle)
    {
        std::cout << "open file failed" << std::endl;
        exit(-1);
    }
    return handle;
}

hdfsFile get_rw_handle(const char *path, hdfsFS fs)
{
    hdfsFile handle = hdfsOpenFile(fs, path, O_RDWR | O_CREAT, 0, 0, 0);
    if(!handle)
    {
        std::cout << "open file failed" << std::endl;
        exit(-1);
    }
    return handle;
}

LineReader::LineReader()
{

}

LineReader::LineReader(hdfsFS& fs, hdfsFile& handle)
{
    this->fs = fs;
    this->handle = handle;
    this->file_end = false;
    this->length = 0;
    this->size = LINE_DEFAULT_SIZE;
    fill();
    this->line = (char *)malloc(LINE_DEFAULT_SIZE);
}

LineReader::~LineReader()
{
    free(this->line);
}

void LineReader::double_linebuf()
{
    this->size *= 2;
    this->line = (char *)realloc(this->line, this->size);
}

void LineReader::line_append(const char *first, int num)
{
    //判断是否可以放下
    while(this->length + num + 1 > this->size)
    {
        this->double_linebuf();
    }
    memcpy(this->line + this->length, first, num);
    this->length += num;
}

void LineReader::fill()
{
    //将数据读取到缓冲区中
    this->buf_size = hdfsRead(this->fs, this->handle, this->buf, HDFS_BUF_SIZE);
    //读取失败
    if(this->buf_size == -1)
    {
        std::cout << "read file failed" << std::endl;
        exit(-1);
    }
    //更新缓存起始位置为0
    this->buf_pos = 0;
    //如果返回的读取字数<HDFS_BUF_SIZE，则说明文件已经读完
    if(this->buf_size < HDFS_BUF_SIZE)
    {
        this->file_end = true;
    }
}

bool LineReader::eof()
{
    return this->file_end && this->length == 0;
}

void LineReader::append_line()
{   
    //缓冲区为空
    if (buf_pos == buf_size)
		return;
    //查找该范围内换行符的字符的对应位置
	char* pch = (char*)memchr(this->buf + buf_pos, '\n', buf_size - buf_pos);
	//缓冲区中还没有找到换行符
    if (pch == NULL)
	{
        //将缓冲区中剩余的字符串添加到line中
		line_append(buf + buf_pos, buf_size - buf_pos);
        //设置缓冲区的起始位置为缓冲区大小
		buf_pos = buf_size;
        //如果文件未读完，接着填充
		if (!file_end)
			fill();
		else
			return;
        //接着循环，将数据写入到line中
		pch = (char*)memchr(buf, '\n', buf_size);
		while (pch == NULL)
		{
			line_append(buf, buf_size);
			if (!file_end)
				fill();
			else
				return;
			pch = (char*)memchr(buf, '\n', buf_size);
		}
	}
	int validLen = pch - buf - buf_pos;
	line_append(buf + buf_pos, validLen);
	buf_pos += validLen + 1;
	if (buf_pos == buf_size)
	{
		if (!file_end)
			fill();
		else
			return;
	}
    std::cout << length << std::endl;
}

void LineReader::read_line()
{
    length = 0;
    append_line();
}

char *LineReader::get_line()
{
    line[length] = '\0';
    return line;
}

LineWriter::LineWriter()
{

}

LineWriter::LineWriter(const char *path, hdfsFS fs, int m_ID)
{
    this->fs = fs;
    this->path = path;
    this->m_ID = m_ID;
    this->handle = nullptr;
    this->next_part = 0;
    this->cur_size = 0;
    new_file_handle();
}

LineWriter::~LineWriter()
{
    if(hdfsFlush(fs, handle))
    {
        std::cout << "flush failed" << std::endl;
        exit(-1);
    }
    hdfsCloseFile(fs, handle);
}

void LineWriter::new_file_handle()
{
    //设置文件名
    char fname[20];
    strcpy(fname, "part_");
    char buffer[10];
    //如果当前机器ID大于等于0，则根据机器名称添加部分信息
    if(m_ID >= 0)
    {
        sprintf(buffer, "%d", m_ID);
        strcat(fname, buffer);
        strcat(fname, "_");
    }
    sprintf(buffer, "%d", next_part);
    strcat(fname, buffer);
    //刷新缓存
    if(next_part > 0)
    {
        if(hdfsFlush(fs, handle))
        {
            std::cout << "flush failed" << std::endl;
            exit(-1);
        }
        hdfsCloseFile(fs, handle);
    }
    //获取新文件连接
    next_part++;
    cur_size = 0;
    //生成新文件名称
    char* file_path = new char[strlen(path) + strlen(fname) + 2];
    strcpy(file_path, path);
    strcat(file_path, "/");
    strcat(file_path, fname);
    //获取连接
    handle = get_w_handle(file_path, fs);
    delete[] file_path;
}

void LineWriter::write_line(char *line, int num)
{
    if(cur_size + num + 1 > HDFS_BLOCK_SIZE)
    {
        new_file_handle();
    }

    // for(int i = 0; i < num; i++)
    // {
    //     printf("%c", line[i]);
    // }
    // printf("\n");

    //获取写入的数据大小
    size_t numWritten = hdfsWrite(fs, handle, line, num);
    //如果当前写入失败
    if(numWritten == -1)
    {
        std::cout << "write failed" << std::endl;
        exit(-1);
    }
    cur_size += numWritten;
    numWritten = hdfsWrite(fs, handle, "\n", 1);
    if(numWritten == -1)
    {
        std::cout << "write failed" << std::endl;
        exit(-1);
    }
    cur_size += 1;
}

void put(const char *local_path, const char *hdfspath)
{
    //获取文件连接
    hdfsFS fs = get_hdfs_fs();
    hdfsFS lfs = get_local_fs();

    //判断文件是否存在，不存在则报错
    // int flag = hdfsExists(fs, hdfspath);
    // if(flag == 1)
    // {
    //     std::cout << "error" << std::endl;
    //     exit(-1);
    // }

    //获取文件权柄
    hdfsFile in = get_r_handle(local_path, lfs);

    //创建读写类
    LineReader* reader  = new LineReader(lfs, in);
    LineWriter* writer = new LineWriter(hdfspath, fs, -1);

    //进行循环读写
    while(true)
    {
        //读取linux系统中的一行
        reader->read_line();

        //判断是否结束
        if(!reader->eof())
        {
            //写入hdfs中
            writer->write_line(reader->get_line(), reader->length);
        }
        else
        {
            break;
        }
    }
    //关闭连接
    if(!in)
    {
        hdfsCloseFile(lfs, in);
        in = nullptr;
    }
    //释放资源
    delete reader;
    delete writer;
    
    if(!lfs)
    {
        hdfsDisconnect(lfs);
        lfs = nullptr;
    }
    
    if(!fs)
    {
        hdfsDisconnect(fs);
        fs = nullptr;
    }
}

const char* rfind(const char* str, char delim)
{
    int len = strlen(str);
    int pos = 0;
    for (int i = len - 1; i >= 0; i--) {
        if (str[i] == delim) {
            pos = i;
            break;
        }
    }
    return str + pos;
}
