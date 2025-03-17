/////////////////  原始包含
#include <cstdio>
#include <cassert>
#include <cstdlib>
/////////////////  新增包含
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>
#include <numeric>
using namespace std;

/////////////////  前置工具函数

//排序vector返回索引函数
template <typename T>
vector<size_t> sort_indexes(const vector<T>& v) {
    // 初始化索引向量
    vector<size_t> idx(v.size());
    //使用iota对向量赋0~？的连续值
    iota(idx.begin(), idx.end(), 0);
    // 通过比较v的值对索引idx进行排序
    sort(idx.begin(), idx.end(), [&v](size_t i1, size_t i2) { return v[i1] < v[i2]; });
    return idx;
}

/////////////////  原始定义 
#define MAX_DISK_NUM (10 + 1)
#define MAX_DISK_SIZE (16384 + 1)
#define MAX_REQUEST_NUM (30000000 + 1)
#define MAX_OBJECT_NUM (100000 + 1)
#define REP_NUM (3)
#define FRE_PER_SLICING (1800)
#define EXTRA_TIME (105)

typedef struct Request_ {
    int object_id;
    int prev_id;
    int sz;
    bool have_read[5] = { false };
    bool is_done;
} Request;

typedef struct Object_ {
    //int replica[REP_NUM + 1];
    int replica_disk[REP_NUM];
    int replica_partition[REP_NUM];
    bool replica_over_write[REP_NUM] = { false };
    //int* unit[REP_NUM + 1];
    vector<int> store_units[REP_NUM];
    int size;
    int last_request_point;
    int tag;
    bool is_delete = false;
    vector<int> unfinished_requests;
} Object;





/////////////////  新增定义



//定义一块磁盘的有关属性
struct Partition {
    //磁盘的子区
    int tag = 0;
    int sz = 0;
    int first_unit = -1;
    int last_unit = -1;
    bool reverse_write = false;
    bool unassigned = true;
    int init_pointer = 0;
    int reverse_pointer = 0;
    bool full = false;
    Partition() {};
    Partition(int tag_, int sz_, int first_unit_, bool reverse_write_, bool unassigned_) {
        tag = tag_;
        sz = sz_;
        first_unit = first_unit_;
        last_unit = first_unit_ + sz_ - 1;
        reverse_write = reverse_write_;
        unassigned = unassigned_;
        if (reverse_write) {
            init_pointer = last_unit;
        }
        else {
            init_pointer = first_unit;
        }
        if (reverse_write) {
            reverse_pointer = first_unit;
        }
        else {
            reverse_pointer = last_unit;
        }
    }
};

struct Disk {
    //分区
    vector<Partition> partitions; // []
    vector<vector<int>> tag_sequence; // 用于存储tag的分区编号
    int capacity = 0;
    int spare_units = 0;
    vector<int> unit_status;

    Disk() {};
    Disk(int capa) {
        partitions.resize(1);
        capacity = capa;
        spare_units = capacity;
        unit_status.resize(capa);
        for (int i = 0; i < capa; i++) {
            unit_status[i] = 0;
        }
    }
};

/////////////////  原始全局变量
int T, M, N, V, G;
int disk[MAX_DISK_NUM][MAX_DISK_SIZE];
int disk_point[MAX_DISK_NUM];

Request request[MAX_REQUEST_NUM];
Object object[MAX_OBJECT_NUM];

ifstream file("sample_practice.txt");
bool debug_mode = true;
int ts = 0;

/////////////////  新增全局变量

vector<vector<int>> fre_del;
vector<vector<int>> fre_write;
vector<vector<int>> fre_read;

vector<Disk> disks;



/////////////////  原始函数

// 对齐时间片
void timestamp_action()
{
    int timestamp;
    
    ts++;
    
    if (debug_mode) {
        string line;
        getline(file, line);
        printf("%s\n", line.c_str());
    }
    else {
        scanf("%*s%d", &timestamp);
        printf("TIMESTAMP %d\n", timestamp);
    }
    
    

    fflush(stdout);
}

// 对象删除步
void do_object_delete(const int* object_unit, int* disk_unit, int size)
{
    for (int i = 1; i <= size; i++) {
        disk_unit[object_unit[i]] = 0;
    }
}

// 对象删除操作
void delete_action()
{
    int n_delete;
    int abort_num = 0;
    //读取当前ts删除操作的总数
    if (debug_mode) {
        string line;
        getline(file, line);
        stringstream ss(line);
        ss >> n_delete;
    }
    else {
        scanf("%d", &n_delete);
    }

    for (int i = 1; i <= n_delete; i++) {
        int id = 0;
        // 读取删除对象的id
        if (debug_mode) {
            string line;
            getline(file, line);
            stringstream ss(line);
            ss >> id;
        }
        else{
            scanf("%d", id);
        }
        // 执行删除操作
        object[id].is_delete = true;

        int size = object[id].size;
        int tag = object[id].tag;
        for (int i = 0; i < REP_NUM; i++) {
            int rep_d = object[id].replica_disk[i];
            int rep_p = object[id].replica_partition[i];
            int rep_o = object[id].replica_over_write[i];

            // 迁移初始指针
            if (rep_o) {
                if (disks[rep_d].partitions[rep_p].reverse_write) {
                    if (disks[rep_d].partitions[rep_p].reverse_pointer> object[id].store_units[i][0]) {
                        disks[rep_d].partitions[rep_p].reverse_pointer = object[id].store_units[i][0];
                    }
                }
                else {
                    if (disks[rep_d].partitions[rep_p].reverse_pointer < object[id].store_units[i][0]) {
                        disks[rep_d].partitions[rep_p].reverse_pointer = object[id].store_units[i][0];
                    }
                }
            }
            else {
                if (disks[rep_d].partitions[rep_p].reverse_write) {
                    if (disks[rep_d].partitions[rep_p].init_pointer < object[id].store_units[i][0]) {
                        disks[rep_d].partitions[rep_p].init_pointer = object[id].store_units[i][0];
                    }
                }
                else {
                    if (disks[rep_d].partitions[rep_p].init_pointer > object[id].store_units[i][0]) {
                        disks[rep_d].partitions[rep_p].init_pointer = object[id].store_units[i][0];
                    }
                }
            }
            
            // 释放unit
            for (int j = 0; j < size; j++) {
                int u = object[id].store_units[i][j];
                disks[rep_d].unit_status[u] = 0;
                disks[rep_d].spare_units += size;
            }

            // 更新partition状态
            disks[rep_d].partitions[rep_p].full = false;
        }
    }


    //int n_delete;
    //int abort_num = 0;
    //static int _id[MAX_OBJECT_NUM];
    ////读取当前ts删除操作的总数
    //scanf("%d", &n_delete);

    //for (int i = 1; i <= n_delete; i++) {
    //    scanf("%d", &_id[i]);
    //}

    //for (int i = 1; i <= n_delete; i++) {
    //    int id = _id[i];
    //    int current_id = object[id].last_request_point;
    //    while (current_id != 0) {
    //        if (request[current_id].is_done == false) {
    //            abort_num++;
    //        }
    //        current_id = request[current_id].prev_id;
    //    }
    //}

    //printf("%d\n", abort_num);
    //for (int i = 1; i <= n_delete; i++) {
    //    int id = _id[i];
    //    int current_id = object[id].last_request_point;
    //    while (current_id != 0) {
    //        if (request[current_id].is_done == false) {
    //            printf("%d\n", current_id);
    //        }
    //        current_id = request[current_id].prev_id;
    //    }
    //    for (int j = 1; j <= REP_NUM; j++) {
    //        //do_object_delete(object[id].unit[j], disk[object[id].replica[j]], object[id].size);
    //    }
    //    object[id].is_delete = true;
    //}
    //
    //fflush(stdout);
}

//对象写入步
//void do_object_write(int* object_unit, int* disk_unit, int size, int object_id)
//{
//    int current_write_point = 0;
//    for (int i = 1; i <= V; i++) {
//        if (disk_unit[i] == 0) {
//            disk_unit[i] = object_id;
//            object_unit[++current_write_point] = i;
//            if (current_write_point == size) {
//                break;
//            }
//        }
//    }
//
//    assert(current_write_point == size);
//}

//对象写入操作
void write_action()
{
    int n_write;
    if (debug_mode) {
        string line;
        getline(file, line);
        stringstream ss(line);
        ss >> n_write;
    }
    else {
        scanf("%d", &n_write);
    }

    for (int i = 1; i <= n_write; i++) {
        //读取写入信息
        int id, size, tag;
        if (debug_mode) {
            string line;
            getline(file, line);
            stringstream ss(line);
            ss >> id >> size >> tag;
        }
        else {
            scanf("%d%d%d", &id, &size, &tag);
        }
        
        //存储到object
        object[id].last_request_point = 0;
        object[id].size = size;
        object[id].tag = tag;

        //打乱遍历disk顺序
        vector<int> disk_iter_sequence;
        disk_iter_sequence.resize(N);
        for (int n = 0; n < N; n++) {
            disk_iter_sequence[n] = n;
        }
        random_shuffle(disk_iter_sequence.begin(), disk_iter_sequence.end());
        //记录已经完成存储的备份数量
        int store_rep = 0;
        int store_requirement = REP_NUM;
        for (int n = 0; n < N; n++) {
            int choose_disk = disk_iter_sequence[n];
            bool success_stored = false;//用于记录是否顺利存储
            for (auto k = disks[choose_disk].tag_sequence[tag].begin(); k != disks[choose_disk].tag_sequence[tag].end(); k++) {
                int start_unit = disks[choose_disk].partitions[*k].first_unit;
                int last_unit = disks[choose_disk].partitions[*k].last_unit;
                if (disks[choose_disk].partitions[*k].full) {
                    continue;
                }
                if (disks[choose_disk].partitions[*k].reverse_write) {
                    //倒写存储
                    for (int u = disks[choose_disk].partitions[*k].init_pointer; u >= start_unit; u--) {
                        if (u - size + 1 < start_unit) {
                            break;
                        }
                        if (disks[choose_disk].unit_status[u] == 0) {
                            bool can_store = true;
                            for (int v = 0; v < size; v++) {
                                if (disks[choose_disk].unit_status[u - v] != 0) {
                                    can_store = false;
                                    break;
                                }
                            }
                            if (can_store) {
                                //可以存储
                                success_stored = true;
                                object[id].replica_disk[store_rep] = choose_disk;
                                object[id].replica_partition[store_rep] = *k;
                                object[id].replica_over_write[store_rep] = false;
                                object[id].store_units[store_rep].resize(size);
                                int iter = 0;
                                for (int v = u; v > u - size; v--) {
                                    disks[choose_disk].unit_status[v] = tag;
                                    object[id].store_units[store_rep][iter] = v;
                                    iter++;
                                }
                                disks[choose_disk].spare_units -= size;
                                store_rep++;
                                //更新写盘指针位置
                                while (disks[choose_disk].unit_status[disks[choose_disk].partitions[*k].init_pointer] != 0) {
                                    disks[choose_disk].partitions[*k].init_pointer--;
                                    if (disks[choose_disk].partitions[*k].init_pointer < disks[choose_disk].partitions[*k].first_unit) {
                                        disks[choose_disk].partitions[*k].full = true;
                                        break;
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
                else {
                    //顺写存储
                    for (int u = disks[choose_disk].partitions[*k].init_pointer; u <= last_unit; u++) {
                        if (u + size - 1 > last_unit) {
                            break;
                        }
                        if (disks[choose_disk].unit_status[u] == 0) {
                            bool can_store = true;
                            for (int v = 0; v < size; v++) {
                                if (disks[choose_disk].unit_status[u + v] != 0) {
                                    can_store = false;
                                    break;
                                }
                            }
                            if (can_store) {
                                //可以存储
                                success_stored = true;
                                object[id].replica_disk[store_rep] = choose_disk;
                                object[id].replica_partition[store_rep] = *k;
                                object[id].replica_over_write[store_rep] = false;
                                object[id].store_units[store_rep].resize(size);
                                int iter = 0;
                                for (int v = u; v < u + size; v++) {
                                    disks[choose_disk].unit_status[v] = tag;
                                    object[id].store_units[store_rep][iter] = v;
                                    iter++;
                                }
                                disks[choose_disk].spare_units -= size;
                                store_rep++;
                                //更新写盘指针位置
                                while (disks[choose_disk].unit_status[disks[choose_disk].partitions[*k].init_pointer] != 0) {
                                    disks[choose_disk].partitions[*k].init_pointer++;
                                    if (disks[choose_disk].partitions[*k].init_pointer > disks[choose_disk].partitions[*k].last_unit) {
                                        disks[choose_disk].partitions[*k].full = true;
                                        break;
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
                if (success_stored) {
                    break;
                }
            }

            if (store_rep == store_requirement) {
                break;
            }
        }

        // 未能完成需要的备份数量时，占用规划范围外的内存。
        if (store_rep < store_requirement) {
            for (int n = 0; n < N; n++) {
                int choose_disk = disk_iter_sequence[n];
                bool success_stored = false;//用于记录是否顺利存储
                for (auto k = disks[choose_disk].tag_sequence[tag].begin(); k != disks[choose_disk].tag_sequence[tag].end(); k++) {
                    int choose_partition = *k;
                    if (disks[choose_disk].partitions[choose_partition].reverse_write) {
                        choose_partition--;
                    }
                    else {
                        choose_partition++;
                    }
                    if (disks[choose_disk].partitions[choose_partition].full){
                        continue;
                    }
                    if (disks[choose_disk].partitions[choose_partition].unassigned) {
                        continue;
                    }
                    int start_unit = disks[choose_disk].partitions[choose_partition].first_unit;
                    int last_unit = disks[choose_disk].partitions[choose_partition].last_unit;
                    if (disks[choose_disk].partitions[choose_partition].reverse_write) {
                        //顺写存储
                        for (int u = disks[choose_disk].partitions[choose_partition].reverse_pointer; u <= last_unit; u++) {
                            if (u + size - 1 > last_unit) {
                                break;
                            }
                            if (disks[choose_disk].unit_status[u] == 0) {
                                bool can_store = true;
                                for (int v = 0; v < size; v++) {
                                    if (disks[choose_disk].unit_status[u + v] != 0) {
                                        can_store = false;
                                        break;
                                    }
                                }
                                if (can_store) {
                                    //可以存储
                                    success_stored = true;
                                    object[id].replica_disk[store_rep] = choose_disk;
                                    object[id].replica_partition[store_rep] = choose_partition;
                                    object[id].replica_over_write[store_rep] = true;
                                    object[id].store_units[store_rep].resize(size);
                                    int iter = 0;
                                    for (int v = u; v < u + size; v++) {
                                        disks[choose_disk].unit_status[v] = tag;
                                        object[id].store_units[store_rep][iter] = v;
                                        iter++;
                                    }
                                    disks[choose_disk].spare_units -= size;
                                    store_rep++;
                                    //更新写盘指针位置
                                    while (disks[choose_disk].unit_status[disks[choose_disk].partitions[choose_partition].reverse_pointer] != 0) {
                                        disks[choose_disk].partitions[choose_partition].reverse_pointer++;
                                        if (disks[choose_disk].partitions[choose_partition].reverse_pointer > disks[choose_disk].partitions[choose_partition].last_unit) {
                                            disks[choose_disk].partitions[choose_partition].full = true;
                                            break;
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    else {
                        //倒写存储
                        for (int u = disks[choose_disk].partitions[choose_partition].reverse_pointer; u >= start_unit; u++) {
                            if (u - size + 1 < start_unit) {
                                break;
                            }
                            if (disks[choose_disk].unit_status[u] == 0) {
                                bool can_store = true;
                                for (int v = 0; v < size; v++) {
                                    if (disks[choose_disk].unit_status[u - v] != 0) {
                                        can_store = false;
                                        break;
                                    }
                                }
                                if (can_store) {
                                    //可以存储
                                    success_stored = true;
                                    object[id].replica_disk[store_rep] = choose_disk;
                                    object[id].replica_partition[store_rep] = choose_partition;
                                    object[id].replica_over_write[store_rep] = true;
                                    object[id].store_units[store_rep].resize(size);
                                    int iter = 0;
                                    for (int v = u; v > u - size; v--) {
                                        disks[choose_disk].unit_status[v] = tag;
                                        object[id].store_units[store_rep][iter] = v;
                                        iter++;
                                    }
                                    disks[choose_disk].spare_units -= size;
                                    store_rep++;
                                    //更新写盘指针位置
                                    while (disks[choose_disk].unit_status[disks[choose_disk].partitions[choose_partition].reverse_pointer] != 0) {
                                        disks[choose_disk].partitions[choose_partition].reverse_pointer--;
                                        if (disks[choose_disk].partitions[choose_partition].reverse_pointer < disks[choose_disk].partitions[choose_partition].first_unit) {
                                            disks[choose_disk].partitions[choose_partition].full = true;
                                            break;
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    if (success_stored) {
                        break;
                    }
                }
                if (store_rep == store_requirement) {
                    break;
                }
            }
        }

        // 若仍未完成需要的备份数量，则占用未分配的分区
        if (store_rep < store_requirement) {
            for (int n = 0; n < N; n++) {
                int choose_disk = disk_iter_sequence[n];
                bool success_stored = false;//用于记录是否顺利存储
                int last_unit = disks[choose_disk].partitions[0].last_unit;
                if (disks[choose_disk].partitions[0].full) {
                    continue;
                }

                for (int u = disks[choose_disk].partitions[0].init_pointer; u <= last_unit; u++) {
                    if (u + size - 1 > last_unit) {
                        break;
                    }
                    bool can_store = true;
                    for (int v = 0; v < size; v++) {
                        if (disks[choose_disk].unit_status[u + v] != 0) {
                            can_store = false;
                            break;
                        }
                    }
                    if (can_store) {
                        //可以存储
                        success_stored = true;
                        object[id].replica_disk[store_rep] = choose_disk;
                        object[id].replica_partition[store_rep] = 0;
                        object[id].replica_over_write[store_rep] = false;
                        object[id].store_units[store_rep].resize(size);
                        int iter = 0;
                        for (int v = u; v < u + size; v++) {
                            disks[choose_disk].unit_status[v] = tag;
                            object[id].store_units[store_rep][iter] = v;
                            iter++;
                        }
                        disks[choose_disk].spare_units -= size;
                        store_rep++;
                        //更新写盘指针位置
                        while (disks[choose_disk].unit_status[disks[choose_disk].partitions[0].init_pointer] != 0) {
                            disks[choose_disk].partitions[0].reverse_pointer++;
                            if (disks[choose_disk].partitions[0].reverse_pointer > disks[choose_disk].partitions[0].last_unit) {
                                disks[choose_disk].partitions[0].full = true;
                                break;
                            }
                        }
                        break;
                    }
                }
                if (store_rep == store_requirement) {
                    break;
                }
            }
        }
        //for (int j = 1; j <= REP_NUM; j++) {

        //    object[id].replica[j] = (id + j) % N + 1;
        //    object[id].unit[j] = static_cast<int*>(malloc(sizeof(int) * (size + 1)));
        //    object[id].size = size;
        //    object[id].is_delete = false;
        //    do_object_write(object[id].unit[j], disk[object[id].replica[j]], size, id);
        //}

        printf("%d\n", id);
        for (int j = 0; j < REP_NUM; j++) {
            printf("%d", object[id].replica_disk[j] + 1);
            for (int k = 0; k < size; k++) {
                printf(" %d", object[id].store_units[j][k] + 1);
            }
            printf("\n");
        }
    }

    fflush(stdout);
}

//对象读取操作
void read_action()
{
    int n_read;
    if (debug_mode) {
        string line;
        getline(file, line);
        stringstream ss(line);
        ss >> n_read;
    }
    else {
        scanf("%d", &n_read);
    }

    int request_id, object_id;
    for (int i = 1; i <= n_read; i++) {
        if (debug_mode) {
            string line;
            getline(file, line);
            stringstream ss(line);
            ss >> request_id >> object_id;
        }
        else {
            scanf("%d%d", &request_id, &object_id);
        }
        request[request_id].object_id = object_id;
        request[request_id].prev_id = object[object_id].last_request_point;
        object[object_id].last_request_point = request_id;
        request[request_id].is_done = false;
    }

    //static int current_request = 0;
    //static int current_phase = 0;
    //if (!current_request && n_read > 0) {
    //    current_request = request_id;
    //}
    //if (!current_request) {
    //    for (int i = 1; i <= N; i++) {
    //        printf("#\n");
    //    }
    //    printf("0\n");
    //}
    //else {
    //    current_phase++;
    //    object_id = request[current_request].object_id;
    //    for (int i = 1; i <= N; i++) {
    //        if (i == object[object_id].replica[1]) {
    //            if (current_phase % 2 == 1) {
    //                printf("j %d\n", object[object_id].unit[1][current_phase / 2 + 1]);
    //            }
    //            else {
    //                printf("r#\n");
    //            }
    //        }
    //        else {
    //            printf("#\n");
    //        }
    //    }

    //    if (current_phase == object[object_id].size * 2) {
    //        if (object[object_id].is_delete) {
    //            printf("0\n");
    //        }
    //        else {
    //            printf("1\n%d\n", current_request);
    //            request[current_request].is_done = true;
    //        }
    //        current_request = 0;
    //        current_phase = 0;
    //    }
    //    else {
    //        printf("0\n");
    //    }
    //}

    //fflush(stdout);
}

//释放内存
void clean()
{
    //for (auto& obj : object) {
    //    for (int i = 1; i <= REP_NUM; i++) {
    //        if (obj.unit[i] == nullptr)
    //            continue;
    //        free(obj.unit[i]);
    //        obj.unit[i] = nullptr;
    //    }
    //}
}


// 初始化读取+预处理
void initialize()
{
    if (debug_mode) {
        string line;
        getline(file, line);
        stringstream ss(line);
        ss >> T >> M >> N >> V >> G;
    }
    else {
        scanf("%d%d%d%d%d", &T, &M, &N, &V, &G);
    }


    // 初始化硬盘信息
    for (int i = 0; i < N; i++) {
        Disk disk1 = Disk(V);
        disks.push_back(disk1);
    }

    fre_del.resize(M);
    fre_write.resize(M);
    fre_read.resize(M);
    int section_number = ceil(float(T) / 1800);
    for (int i = 0; i < M; i++) {
        fre_del[i].resize(section_number);
        fre_write[i].resize(section_number);
        fre_read[i].resize(section_number);
    }

    if (debug_mode) {
        for (int i = 1; i <= M; i++) {
            string line;
            getline(file, line);
            stringstream ss(line);
            for (int j = 1; j <= (T - 1) / FRE_PER_SLICING + 1; j++) {
                ss >> fre_del[i - 1][j - 1];
            }
        }

        for (int i = 1; i <= M; i++) {
            string line;
            getline(file, line);
            stringstream ss(line);
            for (int j = 1; j <= (T - 1) / FRE_PER_SLICING + 1; j++) {
                ss >> fre_write[i - 1][j - 1];
            }
        }

        for (int i = 1; i <= M; i++) {
            string line;
            getline(file, line);
            stringstream ss(line);
            for (int j = 1; j <= (T - 1) / FRE_PER_SLICING + 1; j++) {
                ss >> fre_read[i - 1][j - 1];
            }
        }
    }
    else {
        for (int i = 1; i <= M; i++) {
            for (int j = 1; j <= (T - 1) / FRE_PER_SLICING + 1; j++) {
                scanf("%d", &fre_del[i - 1][j - 1]);
            }
        }

        for (int i = 1; i <= M; i++) {
            for (int j = 1; j <= (T - 1) / FRE_PER_SLICING + 1; j++) {
                scanf("%d", &fre_write[i - 1][j - 1]);
            }
        }

        for (int i = 1; i <= M; i++) {
            for (int j = 1; j <= (T - 1) / FRE_PER_SLICING + 1; j++) {
                scanf("%d", &fre_read[i - 1][j - 1]);
            }
        }
    }

    printf("OK\n");
    fflush(stdout);
}


// 预分配
void disk_pre_allocation(float unassigned_rate, int part_number)
{
    int section_number = ceil(float(T) / 1800); // 时间片的整体数量
    vector<int> tag_max_memory;  // 记录标签数据最大内存
    vector<int> tag_section_memory; // 记录标签数据在每个片段的内存
    tag_max_memory.resize(M);
    tag_section_memory.resize(M);
    for (int i = 0; i < M; i++) {
        tag_section_memory[i] = 0;
        tag_max_memory[i] = 0;
        for (int j = 0; j < section_number; j++) {
            tag_section_memory[i] += fre_write[i][j];
            tag_section_memory[i] -= fre_del[i][j];
            if (tag_section_memory[i] > tag_max_memory[i]) {
                tag_max_memory[i] = tag_section_memory[i];
            }
        }
    }
    //归一化
    int total_memory_need = 0;
    for (int i = 0; i < M; i++) {
        total_memory_need += tag_max_memory[i];
    }
    vector<float> tag_memory_rate;
    tag_memory_rate.resize(M);
    for (int i = 0; i < M; i++) {
        tag_memory_rate[i] = float(tag_max_memory[i]) / total_memory_need;
    }

    //找出一半数据较多的tag和一半数据较少的tag
    vector<int> tag_more;
    vector<int> tag_less;

    vector<size_t> sort_idx_tag_memory = sort_indexes(tag_memory_rate); // 返回排序索引

    for (int i = 0; i < M; i++) {
        sort_idx_tag_memory[i]++;
    }

    for (int i = 0; i < M / 2; i++) {
        tag_less.push_back(sort_idx_tag_memory[i]);
        tag_more.push_back(sort_idx_tag_memory[M - 1 - i]);
    }

    // 开始随机两两组配，给出disk内存分配

    // 记录可分配的内存
    int assign_able_memory = floor((1-unassigned_rate) * V);
    int part_memory = assign_able_memory / part_number;
    for (int i = 0; i < N; i++) {
        int surplus_memory = disks[i].spare_units;
        int first_unit = 0;
        disks[i].tag_sequence.resize(M + 1);
        for (int j = 0; j < part_number; j++) {
            random_shuffle(tag_less.begin(), tag_less.end());
            random_shuffle(tag_more.begin(), tag_more.end());
            for (size_t k = 0; k < tag_less.size(); k++) {
                int tag = tag_more[k];
                int sz = part_memory * tag_memory_rate[tag_more[k] - 1] + 1;
                Partition part1 = Partition(tag, sz, first_unit, false, false);
                disks[i].partitions.push_back(part1);
                disks[i].tag_sequence[tag].push_back(int(disks[i].partitions.size())-1);
                first_unit += sz;
                surplus_memory -= sz;
                tag = tag_less[k];
                sz = part_memory * tag_memory_rate[tag_less[k] - 1] + 1;
                Partition part2 = Partition(tag, sz, first_unit, true, false);
                disks[i].partitions.push_back(part2);
                disks[i].tag_sequence[tag].push_back(int(disks[i].partitions.size())-1);
                first_unit += sz;
                surplus_memory -= sz;
            }
            if (M % 2) {
                int tag = sort_idx_tag_memory[M/2];
                int sz = part_memory * tag_memory_rate[tag_more[M / 2] - 1] + 1;
                Partition part1 = Partition(tag, sz, first_unit, false, false);
                disks[i].partitions.push_back(part1);
                disks[i].tag_sequence[tag].push_back(int(disks[i].partitions.size()));
                first_unit += sz;
                surplus_memory -= sz;
            }
        }
        // 剩余内存分配给无tag分区
        disks[i].partitions[0].sz = surplus_memory;
        disks[i].partitions[0].first_unit = first_unit;
        disks[i].partitions[0].last_unit = V - 1;
        disks[i].partitions[0].init_pointer = first_unit;
        disks[i].partitions[0].reverse_pointer = V - 1;
    }

    //// 取一部分用于容错的存储空间，不进行分配
    //for (int i = 0; i < N; i++) {
    //    int a = 1;
    //}
    //// 统计所有tag所有对象的总内存
    //


    //vector<vector<int>> max_tag_size;
    //max_tag_size.resize(M);
    //vector<int> current_tag_size;
    //current_tag_size.resize(M);
    //
    //vector<int> total_max_size;
    //total_max_size.resize(M);
    //int total_size = 0;
    //
    //for (int i = 0; i < M; i++) {
    //    current_tag_size[i] = 0;
    //    total_max_size[i] = 0;
    //    max_tag_size[i].resize(section_number);
    //    for (int j = 0; j < section_number; j++) {
    //        if (j == 0) {
    //            max_tag_size[i][j] = fre_write[i][j];
    //        }
    //        else {
    //            max_tag_size[i][j] = current_tag_size[i] + fre_write[i][j];
    //        }
    //        current_tag_size[i] += fre_write[i][j];
    //        current_tag_size[i] -= fre_del[i][j];
    //        if (max_tag_size[i][j] > total_max_size[i]) {
    //            total_max_size[i] = max_tag_size[i][j];
    //        }
    //    }
    //    total_size += total_max_size[i];
    //}


    //
    //int a = 1;
}


int main()
{
    // 执行初始化
    initialize();

    // 执行预分配
    disk_pre_allocation(0.05, 2);

    for (int i = 1; i <= N; i++) {
        disk_point[i] = 1;
    }

    // 循环时间片操作
    for (int t = 1; t <= T + EXTRA_TIME; t++) {
        
        timestamp_action();//时间片对齐

        delete_action();//对象删除

        write_action();//对象写入

        read_action();//对象读取
    }
    clean();

    return 0;
}
