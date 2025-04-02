
///////////////////////////  原始包含
#pragma region Include
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
#include <cmath>
#include <climits>

using namespace std;
#pragma endregion

///////////////////////////////////////////////// 本地debug模式设置
ifstream file("sample_practice.txt");
bool debug_mode = true;
int ts = 0;

/////////////////////////////////////////////////////// 算法可调参数
float jump_req_num_rate = 0.5;
double randseed = 666; // 随机种子
double num_unfi_req_unit_rate_for_continuous_read = 0.35; // 进行连续读取所需要的任务unit比例
double no_tag_rate = 0.05; // 无tag区的size

/////////////////  前置工具函数
#pragma region ToolFunction
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

// 找到vector中的最大值以及其索引
void find_max_idx(vector<int> vec, int* max_value, int* max_idx) {
    *max_value = INT_MIN;
    *max_idx = -1;
    int idx = 0;
    for (auto iter = vec.begin(); iter != vec.end(); iter++) {
        if (*iter > *max_value) {
            *max_value = *iter;
            *max_idx = idx;
        }
        idx++;
    }
}

double calculateVariance(const std::vector<double>& data) {
    if (data.empty()) return 0.0;

    double mean = std::accumulate(data.begin(), data.end(), 0.0) / data.size();
    double variance = 0.0;

    for (double x : data) {
        variance += (x - mean) * (x - mean);
    }

    return variance / data.size(); // 若要计算样本方差改为 (data.size() - 1)
}

#pragma endregion

/////////////////////////   全局常数变量
#pragma region Global_ConstVar_and_Definition
/////////////////  原始定义
#define MAX_DISK_NUM (10 + 1)
#define MAX_DISK_SIZE (16384 + 1)
#define MAX_REQUEST_NUM (30000000 + 1)
#define MAX_OBJECT_NUM (100000 + 1)
#define REP_NUM (3)
#define FRE_PER_SLICING (1800)
#define EXTRA_TIME (105)

#define MAX_OBJECT_SIZE (5) //新增

/////////////////  原始全局变量
int Total_TS, Num_Tags, Num_Disks, SZ_Disks, Num_Tokens, Num_Exchange;

#pragma endregion

///////////////////////////  结构数组定义  Request  Object  Unit  Partition  Disk  Tag_To_Partition
#pragma region Struct_Definition

typedef struct Request_ {
    int object_id;
    int prev_id;
    int sz;
    int arrival_ts;
    bool have_read[MAX_OBJECT_SIZE] = { false }; //新增
    bool is_done = false;
    bool lose_value = false;
} Request;

typedef struct Object_ {
    int replica_disk[REP_NUM];
    int replica_partition[REP_NUM];
    bool replica_over_write[REP_NUM] = { false };
    int store_units[REP_NUM][MAX_OBJECT_SIZE];
    int size;
    int last_request_point;
    int tag;
    int write_in_ts = -1;
    int delete_ts = -1;
    bool is_delete = false;
    bool is_wasted = false;
    int last_req_ts = -1;
} Object;




Request request[MAX_REQUEST_NUM];
Object object[MAX_OBJECT_NUM];




/////////////////  新增定义


//定义一块磁盘的有关属性
struct Unit {
    //磁盘的一个block
    bool replica_assigned = false; // 是否有被指派replica
    bool occupied = false;; // 是否是被obj占据的单元
    int obj_id = 0; // 该单元存储的object id
    int obj_phase = -1; // 该单元存储object的哪一个block
    int rep_disk_id[REP_NUM - 1] = { -1,-1 }; // 该单元存储object的其他replica所在磁盘编号
    int rep_part_id[REP_NUM - 1] = { -1,-1 };
    int rep_unit_id[REP_NUM - 1] = { -1,-1 }; // 该单元存储object的其他replica所在磁盘unit号
    int nxt_unit_id = 0; // 该单元下一个unit号
    int number_unfi_req = 0; // 该单元所存储的未完成的请求数量
    int partition_id = 0;

    Unit() {
        for (int i = 0; i < REP_NUM - 1; i++) {
            rep_disk_id[i] = -1;
            rep_part_id[i] = -1;
        }
    }
};

struct Partition {
    //磁盘的子区
    int tag = 0;
    int sz = 0;
    int occupied_u_number = 0;
    int first_unit = -1;
    int last_unit = -1;
    bool reverse_write = false;
    bool unassigned = true;
    int init_pointer = 0;
    int reverse_pointer = 0;
    bool full = false;
    int num_req = 0;
    bool on_heat = false; // 该子区是否为被主要读取的对象
    int rep_disk_id[REP_NUM - 1] = { 0 }; // 该单元存储object的其他replica所在磁盘编号
    int rep_part_id[REP_NUM - 1] = { 0 }; // 该单元存储object的其他replica所在磁盘unit号
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
    //磁盘
    vector<Partition> partitions; // 分区

    vector<Unit> units; // 单元

    int pointer_location = 0; // 磁盘pointer当前所在的unit

    bool execute_jump = false;  // 是否需要执行jump

    int jump_unit = 0;  // jump的目标unit

    string action_flow = ""; // 执行动作留

    int num_spare_units = 0; //指针是否正空闲


    /// <用于记录disk指针运动规划的变量>
    vector<int> read_plan; // 记录执行read的位置
    vector<int> read_obj; // 对应read_plan，记录read的object
    vector<int> read_phase; // 记录read_plan
    int read_plan_pointer = 0; // 记录一个read_plan当中read的次数
    int plan_over_mark = 0;// 记录action_flow_plan结束的位置
    /// </用于记录disk指针运动规划的变量>

    Disk() {};
    Disk(int capacity, int max_continuous_read) {
        partitions.resize(1);
        units.resize(capacity + 1);
        num_spare_units = capacity;
        pointer_location = 1;
        for (int i = 1; i < capacity; i++) {
            units[i].nxt_unit_id = i + 1;
        }
        units[capacity].nxt_unit_id = 1;
        units[0].occupied = true;

        read_plan.resize(max_continuous_read);
        read_obj.resize(max_continuous_read);
        read_phase.resize(max_continuous_read);

    }
};

struct Tag_To_Partition {
    // 用于记录一个tag存储于哪些partition
    vector<int> disk_list;
    vector<int> partition_list;
    vector<int> over_write_partition_list;
};

#pragma endregion

////////////////// 全局状态变量
#pragma region Global_StatusVar_and_Definition

/////////////////  新增全局变量


vector<Disk> disks;
vector<Tag_To_Partition> tag_to_partition;
vector<int> require_token; // 记录连续读取的token消耗
vector<int> require_token_accumulated;
int max_continuous_read = 0;
int min_pass_read = 0;

vector<double>request_sz_unit_value; // 记录不同size的request每个unit的分数
vector<double>request_sz_unit_value_ts_loss; // 记录不同size的request每个unit的分数 平均每个ts损失多少分数

vector<string> dft_action_flow; // 默认action_flow

// 用于记录request是否失去价值

vector<int>  lose_value_req_loop[EXTRA_TIME];
int loop_pointer[EXTRA_TIME] = { 0 };
int time_pointer = -1;
int loop_unit_size = 30000;

// 用于生成随机的磁盘遍历顺序
vector<int> disk_sequence;

// 用于记录要输出的已完成的任务
vector<int> finished_requests;
int finished_request_pt = 0;

string output_cache;

// 用于记录每个period，每个tag的读取是否on_heat
vector<vector<bool>> tag_on_heat;

#pragma endregion

///////////////////////////////////////////////////////////////////    函数部分    /////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////    函数部分    /////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////    函数部分    /////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////    函数部分    /////////////////////////////////////////////////////////////////////////////////////////

//////////////////  对象读取操作 （子函数）  执行磁盘指针动作的输出                        尚未更新为复赛版本！！！！！！！！！！！！！
void print_pointer_action_and_finished_requsets() {

    output_cache.clear();
    for (int d = 0; d < Num_Disks; d++) {
        if (disks[d].execute_jump) {
            output_cache += "j " + to_string(disks[d].jump_unit) + "\n";
            disks[d].execute_jump = false;
        }
        else {
            output_cache += disks[d].action_flow;
        }
    }

    output_cache += to_string(finished_request_pt) + "\n";

    for (int i = 0; i < finished_request_pt; i++) {
        output_cache += to_string(finished_requests[i]);  // 拼接字符串或数字
        output_cache += "\n";
    }


    printf(output_cache.c_str());

    finished_request_pt = 0;
    fflush(stdout);
}


void execute_diskhead_jump(vector<bool> disk_need_jump, vector<int> head_start_unit, vector<bool> diskhead_have_planned) {
    // 执行jump
    for (int d = 0; d < Num_Disks; d++) {
        if (diskhead_have_planned[d]) {
            continue;
        }
        if (disk_need_jump[d]) {
            disks[d].execute_jump = true;
            disks[d].jump_unit = head_start_unit[d];
            disks[d].pointer_location = disks[d].jump_unit;
        }
    }
}

//////////////////  对象读取操作（子函数的子函数） object、request状态更新                        尚未更新为复赛版本！！！！！！！！！！！！！
void update_request_status(int d) {
    // 更新被读取的object状态和request状态
    int obj_id = disks[d].read_obj[0];
    vector<int> read_phase;
    read_phase.resize(MAX_OBJECT_SIZE);
    int iter = 0;
    read_phase[iter] = disks[d].read_phase[0];
    iter++;
    for (int i = 1; i <= disks[d].read_plan_pointer; i++) {
        if (i == disks[d].read_plan_pointer || (disks[d].read_obj[i] != obj_id && disks[d].read_obj[i] != 0 && obj_id != 0)) {
            // 执行一次request更新
            vector<bool>phase_read_or_not;
            phase_read_or_not.resize(MAX_OBJECT_SIZE);
            for (int j = 0; j < MAX_OBJECT_SIZE; j++) {
                phase_read_or_not[j] = false;
            }
            for (int j = 0; j < iter; j++) {
                phase_read_or_not[read_phase[j]] = true;
            }
            int obj_sz = object[obj_id].size;
            int szidx = obj_sz - 1;
            int last_req = object[obj_id].last_request_point;
            // 逐一校验任务
            vector<double> phase_finish_value;
            phase_finish_value.resize(obj_sz);
            vector<int> phase_finish_num;
            phase_finish_num.resize(obj_sz);
            for (int j = 0; j < obj_sz; j++) {
                phase_finish_value[j] = 0;
                phase_finish_num[j] = 0;
            }
            while (true) {
                if (request[last_req].is_done) {
                    break;
                }
                if (!request[last_req].lose_value) {
                    int surplus_period = EXTRA_TIME - (ts - request[last_req].arrival_ts);
                    for (int j = 0; j < obj_sz; j++) {
                        // 完成该unit的request，扣除value
                        if (phase_read_or_not[j] && !request[last_req].have_read[j]) {
                            phase_finish_value[j] += surplus_period * request_sz_unit_value_ts_loss[szidx];
                            phase_finish_num[j]++;
                        }
                    }
                }

                bool all_read = true;
                for (int j = 0; j < obj_sz; j++) {
                    if (phase_read_or_not[j] || request[last_req].have_read[j]) {
                        request[last_req].have_read[j] = true;
                    }
                    else {
                        all_read = false;
                    }
                }

                if (all_read) {
                    request[last_req].is_done = true;
                    finished_requests[finished_request_pt] = last_req;
                    finished_request_pt++;
                }
                last_req = request[last_req].prev_id;
            }


            //
            iter = 0;

            if (i != disks[d].read_plan_pointer) {
                obj_id = disks[d].read_obj[i];
                read_phase[iter] = disks[d].read_phase[i];
                iter++;
            }
        }
        else {
            if (obj_id != 0 && disks[d].read_obj[i] != 0) {
                read_phase[iter] = disks[d].read_phase[i];
                iter++;
            }
            if (obj_id == 0 && disks[d].read_obj[i] != 0) {
                iter = 0;
                obj_id = disks[d].read_obj[i];
                read_phase[iter] = disks[d].read_phase[i];
                iter++;
            }
        }
    }

    disks[d].read_plan_pointer = 0;
}

//////////////////  对象读取操作（子函数的子函数） 执行磁盘头规划（非连续读取）                        尚未更新为复赛版本！！！！！！！！！！！！！
void execute_pass_read(int d, int token_left, int pt, int num_actions) {
    // pass read
    int continuous_read = 0;
    while (token_left - require_token[continuous_read] >= 0) {
        if (disks[d].units[pt].number_unfi_req > 0 && disks[d].units[pt].occupied) {
            // read 一次
            token_left -= require_token[continuous_read];  //扣除token_left
            num_actions++;  // 增加action数
            continuous_read++; // 增加连续读取数
            disks[d].read_plan[disks[d].read_plan_pointer] = num_actions; // 更新action flow的read标记
            disks[d].read_obj[disks[d].read_plan_pointer] = disks[d].units[pt].obj_id;
            disks[d].read_phase[disks[d].read_plan_pointer] = disks[d].units[pt].obj_phase;
            disks[d].read_plan_pointer++;

            int fi_req = disks[d].units[pt].number_unfi_req;
            disks[d].partitions[disks[d].units[pt].partition_id].num_req -= fi_req;//清空unit其中未读取的任务
            disks[d].units[pt].number_unfi_req = 0; //清空unit其中未读取的任务
            for (int k = 0; k < REP_NUM - 1; k++) {//清空 replica 的 unit其中未读取的任务
                int d2 = disks[d].units[pt].rep_disk_id[k];
                int u = disks[d].units[pt].rep_unit_id[k];
                disks[d2].partitions[disks[d2].units[u].partition_id].num_req -= fi_req;
                disks[d2].units[u].number_unfi_req = 0;
            }
        }
        else {
            //pass 一次
            token_left--;
            num_actions++;
            continuous_read = 0;
        }
        pt = disks[d].units[pt].nxt_unit_id;
    }

    // 剩余token 已不足一次read，向后搜索request unit
    while (token_left > 0) {
        if (disks[d].units[pt].number_unfi_req > 0) {
            break;
        }
        //pass 一次
        token_left--;
        num_actions++;
        pt = disks[d].units[pt].nxt_unit_id;
    }

    // 赋予该ts的指令
    disks[d].plan_over_mark = num_actions;
    disks[d].action_flow = dft_action_flow[num_actions];
    for (int t = 0; t < disks[d].read_plan_pointer; t++) {
        disks[d].action_flow[disks[d].read_plan[t] - 1] = 'r';
    }

    //////////////////////////
    disks[d].pointer_location = pt;
    // 更新request状态
    if (disks[d].read_plan_pointer > 0) {
        update_request_status(d);
    }
    //////////////////////////
}

//////////////////  对象读取操作（子函数的子函数） 执行磁盘头规划（连续读取）                        尚未更新为复赛版本！！！！！！！！！！！！！
void execute_continuous_read(int d, int token_left, int pt, int num_actions) {
    int continuous_read = 0;
    // 剩余token可以read
    while (token_left - require_token[continuous_read] >= 0) {
        //read 一次
        token_left -= require_token[continuous_read];  //扣除token_left
        num_actions++;  // 增加action数
        continuous_read++; // 增加连续读取数
        if (disks[d].units[pt].occupied) {
            disks[d].read_plan[disks[d].read_plan_pointer] = num_actions; // 更新action flow的read标记
            disks[d].read_obj[disks[d].read_plan_pointer] = disks[d].units[pt].obj_id;
            disks[d].read_phase[disks[d].read_plan_pointer] = disks[d].units[pt].obj_phase;
            disks[d].read_plan_pointer++; // 推进read标记指针

            int fi_req = disks[d].units[pt].number_unfi_req;
            disks[d].partitions[disks[d].units[pt].partition_id].num_req -= fi_req;//清空unit其中未读取的任务
            disks[d].units[pt].number_unfi_req = 0; //清空unit其中未读取的任务
            for (int k = 0; k < REP_NUM - 1; k++) {
                int d2 = disks[d].units[pt].rep_disk_id[k];
                int u = disks[d].units[pt].rep_unit_id[k];
                disks[d2].partitions[disks[d2].units[u].partition_id].num_req -= fi_req;
                disks[d2].units[u].number_unfi_req = 0;
            }
        }
        else {
            disks[d].read_plan[disks[d].read_plan_pointer] = num_actions;
            disks[d].read_obj[disks[d].read_plan_pointer] = 0;
            disks[d].read_phase[disks[d].read_plan_pointer] = -1;
            disks[d].read_plan_pointer++; // 推进read标记指针
        }
        pt = disks[d].units[pt].nxt_unit_id;
    }
    // 剩余token 已不足一次read，向后搜索request unit
    while (token_left > 0) {
        if (disks[d].units[pt].number_unfi_req > 0) {
            break;
        }
        //pass 一次
        token_left--;
        num_actions++;
        pt = disks[d].units[pt].nxt_unit_id;
    }
    // 赋予该ts的指令
    disks[d].plan_over_mark = num_actions;
    disks[d].action_flow = dft_action_flow[num_actions];
    for (int t = 0; t < disks[d].read_plan_pointer; t++) {
        disks[d].action_flow[disks[d].read_plan[t] - 1] = 'r';
    }

    //////////////////////////
    disks[d].pointer_location = pt;
    // 更新request状态
    if (disks[d].read_plan_pointer > 0) {
        update_request_status(d);
    }
    //////////////////////////
}

//////////////////  对象读取操作（子函数的子函数） 计算可以连续读取的block数量                        尚未更新为复赛版本！！！！！！！！！！！！！
int calc_max_continuous_read(int token_left) {
    int max_continuous_read = 0;
    for (auto x = require_token_accumulated.begin(); x != require_token_accumulated.end(); x++) {
        if (token_left < *(x)) {
            break;
        }
        else {
            max_continuous_read++;
        }
    }
    return max_continuous_read;
}

//////////////////  对象读取操作（子函数的子函数） 执行磁盘头read方案的规划                        尚未更新为复赛版本！！！！！！！！！！！！！
void give_diskhead_actionflow(int d, int start_pt) {
    int last_pt;
    int pt = disks[d].pointer_location;
    while (true) {
        if (start_pt == pt) {
            break;
        }
        if (start_pt == 1) {
            last_pt = SZ_Disks;
        }
        else {
            last_pt = start_pt - 1;
        }
        if (disks[d].units[start_pt].obj_phase != 0 && disks[d].units[last_pt].number_unfi_req != 0) {
            start_pt--;
            if (start_pt == 0) {
                start_pt = SZ_Disks;
            }
        }
        else {
            break;
        }
    }
    // 从start_pt开始检查接下来有请求的request数量
    int num_actions = 0;
    int token_left = Num_Tokens;
    while (pt != start_pt) {
        num_actions++;
        pt++;
        if (pt > SZ_Disks) {
            pt = 1;
        }
        token_left--;
    }
    disks[d].pointer_location = pt;

    int still_can_continuous_read = calc_max_continuous_read(token_left);
    int least_num_unfi_req_unit = still_can_continuous_read * num_unfi_req_unit_rate_for_continuous_read + 1;
    int num_unfi_req_unit = 0;
    for (int v = 0; v < still_can_continuous_read; v++) {
        int u = v + pt;
        if (u >= SZ_Disks) {
            u -= SZ_Disks;
        }
        if (disks[d].units[u].number_unfi_req > 0) {
            num_unfi_req_unit++;
        }
    }
    if (num_unfi_req_unit >= least_num_unfi_req_unit) {
        // continuous read
        execute_continuous_read(d, token_left, pt, num_actions);
    }
    else {
        // pass read
        execute_pass_read(d, token_left, pt, num_actions);
    }
}

//////////////////  对象读取操作（子函数的子函数） 选择磁盘头jump或read                        尚未更新为复赛版本！！！！！！！！！！！！！
void give_best_diskhead_plan(vector<bool>* disk_need_jump, vector<int>* head_start_unit, vector<bool>* diskhead_have_planned, bool* did_new_plan) {
    // 按随机顺序为disk读取指针规划行为
    random_shuffle(disk_sequence.begin(), disk_sequence.end());

    (*disk_need_jump).resize(Num_Disks);
    (*head_start_unit).resize(Num_Disks);

    for (int i = 0; i < Num_Disks; i++) {
        int d = disk_sequence[i];
        if ((*diskhead_have_planned)[d]) {
            continue;
        }
        double current_ts_req_num_roe = 0;
        double current_20ts_req_num_roe = 0;


        int pt = disks[d].pointer_location;
        int pt1 = disks[d].pointer_location;
        int statistic = 0;
        while (true) {
            statistic++;
            if (statistic == 20 * max_continuous_read) {
                break;
            }
            if (statistic <= max_continuous_read) {
                current_ts_req_num_roe += disks[d].units[pt1].number_unfi_req;
            }
            current_20ts_req_num_roe += disks[d].units[pt1].number_unfi_req;
            pt1++;
            if (pt1 > SZ_Disks) {
                pt1 = 1;
            }
        }

        current_ts_req_num_roe = current_ts_req_num_roe / max_continuous_read;
        current_20ts_req_num_roe = current_20ts_req_num_roe / 20 / max_continuous_read;

        // 计算partition中req_roe最高的
        int max_partition_req_number = 0;
        double max_partition_req_roe = 0;
        for (int p = 1; p < disks[d].partitions.size(); p++) {
            if (disks[d].partitions[p].sz > 0) {
                double p_req_roe = double(disks[d].partitions[p].num_req) / disks[d].partitions[p].sz;
                if (p_req_roe > max_partition_req_roe) {
                    max_partition_req_roe = p_req_roe;
                    max_partition_req_number = p;
                }
            }
        }
        if (max_partition_req_roe < 0.0000001) {
            (*diskhead_have_planned)[d] = true;
            disks[d].action_flow = "#\n";
            continue;
        }


        int current_partition_number = disks[d].units[pt].partition_id;
        //partition号相等
        bool did_action_plan = false;
        if (current_partition_number == max_partition_req_number) {
            // 直接执行规划
            for (int t = 0; t < Num_Tokens; t++) {
                int u = pt + t;
                if (u > SZ_Disks) {
                    u -= SZ_Disks;
                }
                if (disks[d].units[u].number_unfi_req > 0) {
                    (*disk_need_jump)[d] = false;
                    give_diskhead_actionflow(d, u);
                    (*diskhead_have_planned)[d] = true;
                    did_action_plan = true;
                    *did_new_plan = true;
                    break;
                }
            }
        }

        if (did_action_plan) {
            continue;
        }

        //partition号不等，决策是否jump
        if (max_partition_req_roe * jump_req_num_rate > current_20ts_req_num_roe) {
            // 考虑jump partition
            int jump_u = 0;
            for (int u = disks[d].partitions[max_partition_req_number].first_unit; u <= disks[d].partitions[max_partition_req_number].last_unit; u++) {
                if (disks[d].units[u].number_unfi_req > 0) {
                    jump_u = u;
                    break;
                }
            }
            (*disk_need_jump)[d] = true;
            (*head_start_unit)[d] = jump_u;
        }
        else {
            int start_u = 0;
            // 执行规划
            did_action_plan = false;
            for (int t = 0; t < Num_Tokens; t++) {
                int u = pt + t;
                if (u > SZ_Disks) {
                    u -= SZ_Disks;
                }
                if (disks[d].units[u].number_unfi_req > 0) {
                    (*disk_need_jump)[d] = false;
                    give_diskhead_actionflow(d, u);
                    (*diskhead_have_planned)[d] = true;
                    did_action_plan = true;
                    *did_new_plan = true;
                    break;
                }
            }
            if (!did_action_plan) {
                // 考虑jump partition
                int jump_u = 0;
                for (int u = disks[d].partitions[max_partition_req_number].first_unit; u <= disks[d].partitions[max_partition_req_number].last_unit; u++) {
                    if (disks[d].units[u].number_unfi_req > 0) {
                        jump_u = u;
                        break;
                    }
                }
                (*disk_need_jump)[d] = true;
                (*head_start_unit)[d] = jump_u;
            }
        }
    }
}

//////////////////  对象读取操作（子函数） 规划磁盘指针动作                        尚未更新为复赛版本！！！！！！！！！！！！！
void plan_disk_pointer() {

    vector<bool> diskhead_have_planned;
    diskhead_have_planned.resize(Num_Disks);
    vector<bool> disk_need_jump;
    vector<int> head_start_unit;
    while (true) {
        // 决策适宜进行jump的disk
        bool did_new_plan = false;
        give_best_diskhead_plan(&disk_need_jump, &head_start_unit, &diskhead_have_planned, &did_new_plan);
        if (!did_new_plan) {
            // 没有新的磁盘头行为被规划，执行所有的jump.
            execute_diskhead_jump(disk_need_jump, head_start_unit, diskhead_have_planned);
            break;
        }
        // 若这一个循环执行过磁盘头行为规划，则unit状态可能被更新，需要重新对jump进行规划，直到循环中没有执行新的磁盘头规划
    }
}

////////////////////  对象读取操作（子函数） 存储request                        尚未更新为复赛版本！！！！！！！！！！！！！
void record_request() {
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
        request[request_id].arrival_ts = ts;
        int sz = object[object_id].size;
        request[request_id].sz = sz;
        for (int j = 0; j < sz; j++) {
            request[request_id].have_read[j] = false;
        }
        object[object_id].last_request_point = request_id;

        // 对object 对应的replica的partition和unit增加request_num

        // 记录磁盘每一个unit的读取任务数量
        for (int i = 0; i < REP_NUM; i++) {
            int d = object[object_id].replica_disk[i];
            int p = object[object_id].replica_partition[i];
            disks[d].partitions[p].num_req += object[object_id].size;
            for (int j = 0; j < object[object_id].size; j++) {
                int u = object[object_id].store_units[i][j];
                disks[d].units[u].number_unfi_req++;
            }
        }

        // 更新 request_lose_value_loop
        lose_value_req_loop[time_pointer][loop_pointer[time_pointer]] = request_id;
        loop_pointer[time_pointer]++;
    }
}

/////////////////////// 对象读取操作(母函数) read_action                        尚未更新为复赛版本！！！！！！！！！！！！！
void read_action()
{

    // 存储request
    record_request();

    // 规划磁盘指针动作
    plan_disk_pointer();

    // 执行指针动作
    print_pointer_action_and_finished_requsets();

}

/////////////////////// 对象写入（子函数） 对象写入后处理                        尚未更新为复赛版本！！！！！！！！！！！！！
void postprocess_object_write(int obj_id) {
    // 更新unit相应replica相位和其余信息
    for (int i = 0; i < REP_NUM; i++) {
        int d = object[obj_id].replica_disk[i];
        for (int s = 0; s < object[obj_id].size; s++) {
            int u = object[obj_id].store_units[i][s];
        }
    }
}

//////////////////////  对象写入（子函数）  执行对象写入                        尚未更新为复赛版本！！！！！！！！！！！！！
void execute_object_write(int obj_id, vector<int> w_disk_list, vector<int> w_part_list, vector<vector<int>> w_unit_list, bool over_write) {
    //取出object信息
    int sz = object[obj_id].size;
    for (int r = 0; r < REP_NUM; r++) {
        int d = w_disk_list[r];
        int p = w_part_list[r];
        //取出partition信息
        bool reverse_write = disks[d].partitions[p].reverse_write;
        object[obj_id].replica_disk[r] = d; //记录replica所在的disk
        object[obj_id].replica_partition[r] = p;//记录replica所在disk的partition
        object[obj_id].replica_over_write[r] = over_write;
        object[obj_id].write_in_ts = ts;
        for (int v = 0; v < sz; v++) {
            int u = w_unit_list[r][v];
            if (disks[d].units[u].occupied) {
                //已被占据，抛出错误
                throw runtime_error("写入操作异常，目标unit已被占据");
            }
            disks[d].units[u].occupied = true;  // 占据该unit
            disks[d].units[u].obj_id = obj_id;  // 更新占据该unit的object id
            disks[d].units[u].obj_phase = v;    // 更新占据该unit的object phase
            //disks[d].units[u].value_per_req = request_sz_unit_value[sz - 1]; //计算该unit每个任务的分数
            object[obj_id].store_units[r][v] = u;// 更新object被存储disk上的unit位置
        }
        disks[d].partitions[p].occupied_u_number += sz; //更新partition已被写入的内存

        //更新写盘指针位置
        int pt = 0;
        if (over_write) {
            pt = disks[d].partitions[p].reverse_pointer;
        }
        else {
            pt = disks[d].partitions[p].init_pointer;
        }

        int first_u = disks[d].partitions[p].first_unit;
        int last_u = disks[d].partitions[p].last_unit;
        if ((over_write && reverse_write) || (!over_write) && (!reverse_write)) {
            // 顺推
            if (pt <= last_u) {
                while (disks[d].units[pt].occupied) {
                    pt++;
                    if (pt > last_u) {
                        disks[d].partitions[p].full = true;
                        break;
                    }
                }
            }
        }
        else {
            // 逆推
            if (pt >= first_u) {
                while (disks[d].units[pt].occupied) {
                    pt--;
                    if (pt < first_u) {
                        disks[d].partitions[p].full = true;
                        break;
                    }
                }
            }
        }
        if (over_write) {
            disks[d].partitions[p].reverse_pointer = pt;
        }
        else {
            disks[d].partitions[p].init_pointer = pt;
        }
        disks[d].num_spare_units -= sz;
    }
}

//////////////////////  对象写入（子函数的子函数）  判断对象能否写入一个partition（无需连续）                        尚未更新为复赛版本！！！！！！！！！！！！！
void judge_object_write_in_partition(int d, int p, int sz, bool* can_write, vector<int>* write_unit_list) {
    (*can_write) = false;
    (*write_unit_list).clear();
    (*write_unit_list).resize(sz);
    int pt = 0;
    bool reverse_write = disks[d].partitions[p].reverse_write;

    int first_u = disks[d].partitions[p].first_unit;
    int last_u = disks[d].partitions[p].last_unit;

    int num_free_unit = 0; // 记录连续free的unit数量，用于判断能否完整存储一个object

    for (int u = first_u; u <= last_u; u++) {
        if (disks[d].units[u].occupied) {
            continue;
        }
        else {
            (*write_unit_list)[num_free_unit] = u;
            num_free_unit++;
            if (num_free_unit == sz) {
                *can_write = true;
                break;
            }
        }
    }
}

//////////////////////  对象写入（子函数的子函数）  判断对象能否连续写入该partition                        尚未更新为复赛版本！！！！！！！！！！！！！
void judge_object_continuous_write(int d, int p, bool over_write, int sz, bool* can_write, int* start_write_unit) {
    (*can_write) = false;
    int pt = 0;
    if (over_write) {
        pt = disks[d].partitions[p].reverse_pointer;
    }
    else {
        pt = disks[d].partitions[p].init_pointer;
    }
    bool reverse_write = disks[d].partitions[p].reverse_write;

    int first_u = disks[d].partitions[p].first_unit;
    int last_u = disks[d].partitions[p].last_unit;

    int start_u = 0;
    int num_free_unit = 0; // 记录连续free的unit数量，用于判断当前指针位置能否存储
    if ((over_write && reverse_write) || (!over_write) && (!reverse_write)) {
        // 顺写有两种情况，①非超区写入且非逆写区，②超区写入且为逆写区
        while (pt + sz - 1 <= last_u) {
            if (!disks[d].units[pt].occupied) {
                num_free_unit++;
                start_u = pt;
                if (num_free_unit == sz) {
                    // 满足可写入
                    (*can_write) = true;
                    // 倒退初始写入指针的位置
                    start_u -= (sz - 1);
                    break;
                }
            }
            else {
                num_free_unit = 0;
            }
            pt++;
        }
    }
    else {
        // 除上述俩种情况以外，为逆写
        while (pt - sz + 1 >= first_u) {
            if (!disks[d].units[pt].occupied) {
                num_free_unit++;
                start_u = pt;
                if (num_free_unit == sz) {
                    // 满足可写入
                    (*can_write) = true;
                    break;
                }
            }
            else {
                num_free_unit = 0;
            }
            pt--;
        }
    }
    if (*can_write) {
        *start_write_unit = start_u;
    }
}

//////////////////////  对象写入（子函数）寻找可写盘的位置                        尚未更新为复赛版本！！！！！！！！！！！！！
void find_space_for_write(int id, int size, int tag, bool* can_write, vector<int>* w_disk_list, vector<int>* w_part_list, vector<vector<int>>* w_unit_list, bool* over_write) {

    *can_write = false;
    (*w_disk_list).resize(REP_NUM);
    (*w_part_list).resize(REP_NUM);
    (*w_unit_list).resize(REP_NUM);
    for (int i = 0; i < REP_NUM; i++) {
        (*w_unit_list)[i].resize(size);
    }
    // 生成一个随机遍历序列
    vector<int> rand_sequence;
    rand_sequence.resize(tag_to_partition[tag].disk_list.size());
    for (int r = 0; r < tag_to_partition[tag].disk_list.size(); r++) {
        rand_sequence[r] = r;
    }
    random_shuffle(rand_sequence.begin(), rand_sequence.end());
    //////////////////////////// 第一轮寻找  常规写入
    for (int i = 0; i < tag_to_partition[tag].disk_list.size(); i++) {
        int r = rand_sequence[i];
        int d = tag_to_partition[tag].disk_list[r];
        int p = tag_to_partition[tag].partition_list[r];
        if (disks[d].partitions[p].full || disks[d].partitions[p].sz - disks[d].partitions[p].occupied_u_number + 1 < size) {
            continue;
        }
        int start_write_unit;
        judge_object_continuous_write(d, p, false, size, can_write, &start_write_unit);
        if (*can_write) {
            *over_write = false;
            // 给出所有的replica
            (*w_disk_list)[0] = d;
            (*w_part_list)[0] = p;
            for (int v = 0; v < size; v++) {
                (*w_unit_list)[0][v] = start_write_unit + v;
            }
            for (int j = 0; j < REP_NUM - 1; j++) {
                (*w_disk_list)[j + 1] = disks[d].units[start_write_unit].rep_disk_id[j];
                (*w_part_list)[j + 1] = disks[d].units[start_write_unit].rep_part_id[j];
                for (int v = 0; v < size; v++) {
                    (*w_unit_list)[j + 1][v] = disks[d].units[start_write_unit].rep_unit_id[j] + v;
                }
            }
            break;
        }
    }
    //////////////////////////// 第二轮寻找  超区写入
    if (!(*can_write)) {
        for (int i = 0; i < tag_to_partition[tag].disk_list.size(); i++) {
            int r = rand_sequence[i];
            int d = tag_to_partition[tag].disk_list[r];
            int p = tag_to_partition[tag].over_write_partition_list[r];
            if (disks[d].partitions[p].full) {
                continue;
            }
            int start_write_unit;
            judge_object_continuous_write(d, p, true, size, can_write, &start_write_unit);
            if (*can_write) {
                *over_write = true;
                // 给出所有的replica
                (*w_disk_list)[0] = d;
                (*w_part_list)[0] = p;
                for (int v = 0; v < size; v++) {
                    (*w_unit_list)[0][v] = start_write_unit + v;
                }
                for (int j = 0; j < REP_NUM - 1; j++) {
                    (*w_disk_list)[j + 1] = disks[d].units[start_write_unit].rep_disk_id[j];
                    (*w_part_list)[j + 1] = disks[d].units[start_write_unit].rep_part_id[j];
                    for (int v = 0; v < size; v++) {
                        (*w_unit_list)[j + 1][v] = disks[d].units[start_write_unit].rep_unit_id[j] + v;
                    }
                }
                break;
            }
        }
    }
    //////////////////////////// 第三轮寻找  尝试连续写入无tag标签的分区
    // 优先spare_unit较多的disk
    vector<int> n_spare_u;
    vector<int> disk_sequence1;
    if (!(*can_write)) {
        n_spare_u.resize(Num_Disks);
        disk_sequence1.resize(Num_Disks);
        for (int i = 0; i < Num_Disks; i++) {
            n_spare_u[i] = disks[i].num_spare_units;
        }
        vector<size_t> sort_idx;
        sort_idx = sort_indexes(n_spare_u);
        for (int i = 0; i < Num_Disks; i++) {
            disk_sequence1[i] = sort_idx[Num_Disks - 1 - i];
        }
        random_shuffle(disk_sequence.begin(), disk_sequence.end());
        vector<int> w_unit_list1;

        for (int i = 0; i < Num_Disks; i++) {
            int d = disk_sequence1[i];
            for (int p = 1; p < disks[d].partitions.size(); p++) {
                if (!disks[d].partitions[p].unassigned) {
                    continue;
                }
                if (disks[d].partitions[0].full || disks[d].partitions[p].sz - disks[d].partitions[p].occupied_u_number + 1 < size) {
                    continue;
                }
                int start_write_unit;
                judge_object_continuous_write(d, p, false, size, can_write, &start_write_unit);
                if (*can_write) {
                    *over_write = true;
                    // 给出所有的replica
                    (*w_disk_list)[0] = d;
                    (*w_part_list)[0] = p;
                    for (int v = 0; v < size; v++) {
                        (*w_unit_list)[0][v] = start_write_unit + v;
                        if ((*w_unit_list)[0][v] > SZ_Disks) {
                            (*w_unit_list)[0][v] -= SZ_Disks;
                        }
                    }
                    for (int j = 0; j < REP_NUM - 1; j++) {
                        (*w_disk_list)[j + 1] = disks[d].units[start_write_unit].rep_disk_id[j];
                        if ((*w_disk_list)[j + 1] >= 10) {
                            (*can_write) = false;
                            break;
                        }
                        (*w_part_list)[j + 1] = disks[d].units[start_write_unit].rep_part_id[j];
                        for (int v = 0; v < size; v++) {
                            (*w_unit_list)[j + 1][v] = disks[d].units[start_write_unit].rep_unit_id[j] + v;
                        }
                    }
                    break;
                }
            }
            if (*can_write) {
                break;
            }
        }
    }
    //////////////////////////// 第四轮寻找  尝试非连续写入相应tag标签的分区
    if (!(*can_write)) {
        for (int i = 0; i < tag_to_partition[tag].disk_list.size(); i++) {
            int r = rand_sequence[i];
            int d = tag_to_partition[tag].disk_list[r];
            int p = tag_to_partition[tag].partition_list[r];
            if (disks[d].partitions[p].full || disks[d].partitions[p].sz - disks[d].partitions[p].occupied_u_number + 1 < size) {
                continue;
            }
            vector<int> w_unit_list1;
            judge_object_write_in_partition(d, p, size, can_write, &w_unit_list1);
            if (*can_write) {
                (*w_disk_list)[0] = d;
                (*w_part_list)[0] = p;
                (*w_unit_list)[0] = w_unit_list1;
                for (int j = 0; j < REP_NUM - 1; j++) {
                    for (int v = 0; v < size; v++) {
                        int u = w_unit_list1[v];
                        (*w_disk_list)[j + 1] = disks[d].units[u].rep_disk_id[j];
                        (*w_part_list)[j + 1] = disks[d].units[u].rep_part_id[j];
                        (*w_unit_list)[j + 1][v] = disks[d].units[u].rep_unit_id[j];
                    }
                }
                break;
            }
        }
    }
    //////////////////////////// 第五轮寻找  尝试写入其他tag标签的分区
    if (!(*can_write)) {
        vector<int> tag_iter_sequence;
        tag_iter_sequence.resize(Num_Tags);
        for (int i = 0; i < Num_Tags; i++) {
            tag_iter_sequence[i] = i + 1;
        }
        random_shuffle(tag_iter_sequence.begin(), tag_iter_sequence.end());
        for (int j = 0; j < Num_Tags; j++) {
            int t = tag_iter_sequence[j];
            for (int i = 0; i < tag_to_partition[t].disk_list.size(); i++) {
                int r = rand_sequence[i];
                int d = tag_to_partition[t].disk_list[r];
                int p = tag_to_partition[t].partition_list[r];
                if (d != disk_sequence1[0]) {
                    // 优先spare_unit较多的disk
                    continue;
                }
                if (disks[d].partitions[p].full || disks[d].partitions[p].sz - disks[d].partitions[p].occupied_u_number + 1 < size) {
                    continue;
                }
                int start_write_unit;
                judge_object_continuous_write(d, p, false, size, can_write, &start_write_unit);
                if (*can_write) {
                    *over_write = false;
                    // 给出所有的replica
                    (*w_disk_list)[0] = d;
                    (*w_part_list)[0] = p;
                    for (int v = 0; v < size; v++) {
                        (*w_unit_list)[0][v] = start_write_unit + v;
                        if ((*w_unit_list)[0][v] > SZ_Disks) {
                            (*w_unit_list)[0][v] -= SZ_Disks;
                        }
                    }
                    for (int j = 0; j < REP_NUM - 1; j++) {
                        (*w_disk_list)[j + 1] = disks[d].units[start_write_unit].rep_disk_id[j];
                        if ((*w_disk_list)[j + 1] >= 10) {
                            (*can_write) = false;
                            break;
                        }
                        (*w_part_list)[j + 1] = disks[d].units[start_write_unit].rep_part_id[j];
                        for (int v = 0; v < size; v++) {
                            (*w_unit_list)[j + 1][v] = disks[d].units[start_write_unit].rep_unit_id[j] + v;
                        }
                    }
                    break;
                }
            }
            if (*can_write) {
                break;
            }
        }
    }

    //////////////////////////// 第六轮寻找  写入任意非连续空间
    if (!(*can_write)) {
        // 优先spare_unit较多的disk
        vector<int> w_unit_list1;
        for (int i = 0; i < Num_Disks; i++) {
            int d = disk_sequence1[i];
            for (size_t p = disks[d].partitions.size() - 1; p >= 1; p--) {
                if (disks[d].partitions[p].full || disks[d].partitions[p].sz - disks[d].partitions[p].occupied_u_number + 1 < size) {
                    continue;
                }
                judge_object_write_in_partition(d, p, size, can_write, &w_unit_list1);
                if (*can_write) {
                    (*w_disk_list)[0] = d;
                    (*w_part_list)[0] = p;
                    (*w_unit_list)[0] = w_unit_list1;
                    for (int j = 0; j < REP_NUM - 1; j++) {
                        for (int v = 0; v < size; v++) {
                            int u = w_unit_list1[v];
                            (*w_disk_list)[j + 1] = disks[d].units[u].rep_disk_id[j];
                            (*w_part_list)[j + 1] = disks[d].units[u].rep_part_id[j];
                            (*w_unit_list)[j + 1][v] = disks[d].units[u].rep_unit_id[j];
                        }
                    }
                    break;
                }
            }
            if (*can_write) {
                break;
            }
        }
    }
};

////////////////////// 对象写入（母函数） write_action                        尚未更新为复赛版本！！！！！！！！！！！！！
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

    output_cache.clear();
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

        // 寻找可写入的位置
        bool can_write, over_write; // 是否可写入  是否超区写盘
        vector<int> w_disk_list;
        vector<int> w_part_list;
        vector<vector<int>> w_unit_list;// 盘号 分区号 初始写盘位置

        find_space_for_write(id, size, tag, &can_write, &w_disk_list, &w_part_list, &w_unit_list, &over_write);

        if (can_write) {
            // 顺利找到写入方案
            execute_object_write(id, w_disk_list, w_part_list, w_unit_list, over_write);
        }
        else {
            // 若未顺利找到写入方案，则需要考虑非连续写入。
            throw runtime_error("未成功写入。");
        }

        //////////   写入完毕，整理信息
        postprocess_object_write(id);

        ///////// 输出
        output_cache += to_string(id);
        output_cache += "\n";
        for (int j = 0; j < REP_NUM; j++) {
            output_cache += to_string(object[id].replica_disk[j] + 1);
            for (int k = 0; k < size; k++) {
                output_cache += " " + to_string(object[id].store_units[j][k]);
            }
            output_cache += "\n";
        }
    }
    printf(output_cache.c_str());
    fflush(stdout);
}

////////////////////// 对象删除 delete action                        尚未更新为复赛版本！！！！！！！！！！！！！
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


    int need_to_cancel = 0;
    vector<int> cancel_req_number;
    for (int i = 1; i <= n_delete; i++) {
        int id = 0;
        // 读取删除对象的id
        if (debug_mode) {
            string line;
            getline(file, line);
            stringstream ss(line);
            ss >> id;
        }
        else {
            scanf("%d", &id);
        }
        // 执行删除操作
        object[id].is_delete = true;
        object[id].delete_ts = ts;

        int size = object[id].size;
        int tag = object[id].tag;

        // 上报需要删除的任务并更新任务状态
        int req_id = object[id].last_request_point;


        vector<double> phase_surplus_value;
        phase_surplus_value.resize(size);
        //vector<int> phase_surplus_num;
        //phase_surplus_num.resize(size);
        int szidx = size - 1;
        while (!request[req_id].is_done) {
            // 更新删除后的request状态
            request[req_id].is_done = true;
            if (!request[req_id].lose_value) {
                int surplus_period = EXTRA_TIME - (ts - request[req_id].arrival_ts);
                for (int j = 0; j < size; j++) {
                    if (!request[req_id].have_read[j]) {
                        phase_surplus_value[j] += surplus_period * request_sz_unit_value_ts_loss[szidx];
                        //phase_surplus_num[j]++;
                    }
                }
            }
            for (int j = 0; j < MAX_OBJECT_SIZE; j++) {
                request[req_id].have_read[j] = true;
            }
            // 更新输出要取消的请求列表
            cancel_req_number.push_back(req_id);
            need_to_cancel++;
            req_id = request[req_id].prev_id;
        }



        // 处理存储该object的磁盘
        for (int i = 0; i < REP_NUM; i++) {
            int rep_d = object[id].replica_disk[i];
            int rep_p = object[id].replica_partition[i];
            int rep_o = object[id].replica_over_write[i];

            // 迁移初始指针
            if (rep_o) {
                if (disks[rep_d].partitions[rep_p].reverse_write) {
                    if (disks[rep_d].partitions[rep_p].reverse_pointer > object[id].store_units[i][0]) {
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
                disks[rep_d].units[u].occupied = false;
                disks[rep_d].num_spare_units++;
                disks[rep_d].partitions[rep_p].occupied_u_number--;
                //更新partition中未完成的request数量
                int num_unfi_req = disks[rep_d].units[u].number_unfi_req;
                disks[rep_d].partitions[rep_p].num_req -= num_unfi_req;
                disks[rep_d].units[u].number_unfi_req = 0;
            }
            // 更新partition状态
            disks[rep_d].partitions[rep_p].full = false;

        }
    }

    if (need_to_cancel > 0) {
        output_cache.clear();
        output_cache += to_string(need_to_cancel) + "\n";
        for (int i = 0; i < need_to_cancel; i++) {
            output_cache += to_string(cancel_req_number[i]) + "\n";
        }
        printf(output_cache.c_str());
        fflush(stdout);
    }
    else {
        printf("0\n");
        fflush(stdout);
    }

}

////////////////////// 对齐时间片 timestamp_action   处理超时request                        尚未更新为复赛版本！！！！！！！！！！！！！
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

    // 处理超时request
    time_pointer++;
    if (time_pointer == EXTRA_TIME) {
        time_pointer = 0;
    }

    for (int i = 0; i < loop_pointer[time_pointer]; i++) {
        int req_id = lose_value_req_loop[time_pointer][i];
        if (request[req_id].is_done) {
            continue;
        }
        int obj_id = request[req_id].object_id;
        request[req_id].lose_value = true;
        int sz = request[req_id].sz;
        sz--;
        for (int j = 0; j <= sz; j++) {
            if (!request[req_id].have_read[j]) {
                for (int k = 0; k < REP_NUM; k++) {
                    int d = object[obj_id].replica_disk[k];
                    int p = object[obj_id].replica_partition[k];
                    int u = object[obj_id].store_units[k][j];
                    disks[d].units[u].number_unfi_req--;
                    disks[d].partitions[p].num_req--;
                }
            }
        }
        lose_value_req_loop[time_pointer][i] = 0;
    }
    loop_pointer[time_pointer] = 0;

}

////////////////////// 初始化函数（子函数）初始化磁盘并为tag分区 disk_partition_initialize                        
void disk_partition_initialize() {
    // 初始化硬盘信息
    for (int i = 0; i < Num_Disks; i++) {
        Disk disk1 = Disk(SZ_Disks, max_continuous_read);
        disks.push_back(disk1);
    }

    vector<vector<int>> fre_del;
    vector<vector<int>> fre_write;
    vector<vector<int>> fre_read;
    vector<vector<double>> fre_read_standardized;


    fre_del.resize(Num_Tags);
    fre_write.resize(Num_Tags);
    fre_read.resize(Num_Tags);
    fre_read_standardized.resize(Num_Tags);

    int section_number = ceil(float(Total_TS) / 1800);
    for (int i = 0; i < Num_Tags; i++) {
        fre_del[i].resize(section_number);
        fre_write[i].resize(section_number);
        fre_read[i].resize(section_number);
        fre_read_standardized[i].resize(section_number);
    }

    if (debug_mode) {
        for (int i = 1; i <= Num_Tags; i++) {
            string line;
            getline(file, line);
            stringstream ss(line);
            for (int j = 1; j <= (Total_TS - 1) / FRE_PER_SLICING + 1; j++) {
                ss >> fre_del[i - 1][j - 1];
            }
        }

        for (int i = 1; i <= Num_Tags; i++) {
            string line;
            getline(file, line);
            stringstream ss(line);
            for (int j = 1; j <= (Total_TS - 1) / FRE_PER_SLICING + 1; j++) {
                ss >> fre_write[i - 1][j - 1];
            }
        }

        for (int i = 1; i <= Num_Tags; i++) {
            string line;
            getline(file, line);
            stringstream ss(line);
            for (int j = 1; j <= (Total_TS - 1) / FRE_PER_SLICING + 1; j++) {
                ss >> fre_read[i - 1][j - 1];
            }
        }
    }
    else {
        for (int i = 1; i <= Num_Tags; i++) {
            for (int j = 1; j <= (Total_TS - 1) / FRE_PER_SLICING + 1; j++) {
                scanf("%d", &fre_del[i - 1][j - 1]);
            }
        }

        for (int i = 1; i <= Num_Tags; i++) {
            for (int j = 1; j <= (Total_TS - 1) / FRE_PER_SLICING + 1; j++) {
                scanf("%d", &fre_write[i - 1][j - 1]);
            }
        }

        for (int i = 1; i <= Num_Tags; i++) {
            for (int j = 1; j <= (Total_TS - 1) / FRE_PER_SLICING + 1; j++) {
                scanf("%d", &fre_read[i - 1][j - 1]);
            }
        }
    }

    // 计算16个tag之间 read request数量的相关性
    vector<int> tag_req_max;
    vector<vector<double>> tag_req_correlation;
    tag_req_max.resize(Num_Tags);
    tag_req_correlation.resize(Num_Tags);
    for (int i = 1; i <= Num_Tags; i++) {
        tag_req_max[i - 1] = 0;
        tag_req_correlation[i - 1].resize(Num_Tags);
        for (int j = 1; j <= (Total_TS - 1) / FRE_PER_SLICING + 1; j++) {
            if (fre_read[i - 1][j - 1] > tag_req_max[i - 1]) {
                tag_req_max[i - 1] = fre_read[i - 1][j - 1];
            }
        }
    }
    for (int i = 1; i <= Num_Tags; i++) {
        for (int j = 1; j <= (Total_TS - 1) / FRE_PER_SLICING + 1; j++) {
            fre_read_standardized[i - 1][j - 1] = double(fre_read[i - 1][j - 1]) / tag_req_max[i - 1];
        }
    }
    for (int i = 0; i < Num_Tags; i++) {
        for (int j = 0; j < Num_Tags; j++) {
            vector<double> dist;
            dist.resize((Total_TS - 1) / FRE_PER_SLICING + 1);
            for (int k = 1; k <= (Total_TS - 1) / FRE_PER_SLICING + 1; k++) {
                dist[k - 1] = fre_read_standardized[i][k - 1] - fre_read_standardized[j][k - 1];
            }
            tag_req_correlation[i][j] = calculateVariance(dist);
        }
    }


    // 执行预分配
    tag_to_partition.resize(Num_Tags + 1); // 用于记录每个tag对应的partition所在

    vector<int> tag_max_memory;  // 记录标签数据最大内存
    vector<int> tag_section_memory; // 记录标签数据在每个片段的内存
    tag_max_memory.resize(Num_Tags);
    tag_section_memory.resize(Num_Tags);

    ////计算每个tag最大所需内存
    for (int i = 0; i < Num_Tags; i++) {
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
    ////归一化，计算各tag内存比例
    int total_memory_need = 0;
    for (int i = 0; i < Num_Tags; i++) {
        total_memory_need += tag_max_memory[i];
    }
    vector<float> tag_memory_rate;
    tag_memory_rate.resize(Num_Tags);
    for (int i = 0; i < Num_Tags; i++) {
        tag_memory_rate[i] = float(tag_max_memory[i]) / total_memory_need;
    }

    ////找出一半数据较多的tag和一半数据较少的tag
    vector<int> tag_more;
    vector<int> tag_less;

    vector<size_t> sort_idx_tag_memory = sort_indexes(tag_memory_rate); // 返回排序索引
    for (int i = 0; i < Num_Tags; i++) {
        sort_idx_tag_memory[i]++;
    }
    for (int i = 0; i < Num_Tags / 2; i++) {
        tag_less.push_back(sort_idx_tag_memory[i]);
        tag_more.push_back(sort_idx_tag_memory[Num_Tags - 1 - i]);
    }

    //// 计算每个tag每个分片分别分配多少内存
    vector<int> tag_memory_one_part;
    tag_memory_one_part.resize(Num_Tags);
    for (int i = 0; i < Num_Tags; i++) {
        tag_memory_one_part[i] = ceil(double(SZ_Disks) * (1.0 - no_tag_rate) / REP_NUM * tag_memory_rate[i]);
    }

    //// 给出tag排列顺序
    vector<int> highest_corr_tag;
    vector<double> highest_corr;
    highest_corr_tag.resize(Num_Tags);
    highest_corr.resize(Num_Tags);
    for (int i = 0; i < Num_Tags; i++) {
        highest_corr[i] = 999;
        for (int j = 0; j < Num_Tags; j++) {
            if (j == i) {
                continue;
            }
            if (tag_req_correlation[i][j] < highest_corr[i]) {
                highest_corr[i] = tag_req_correlation[i][j];
                highest_corr_tag[i] = j;
            }
        }
    }
    vector<size_t> lowest_highest_corr_tag = sort_indexes(highest_corr);
    // 从和其他tag相关性最差的tag开始排列两头
    vector<int> tag_sequence;
    tag_sequence.resize(Num_Tags);
    tag_sequence[0] = lowest_highest_corr_tag[Num_Tags - 1] + 1;
    tag_sequence[Num_Tags - 1] = lowest_highest_corr_tag[Num_Tags - 2] + 1;
    vector<bool> tag_allocated;
    tag_allocated.resize(Num_Tags);
    tag_allocated[lowest_highest_corr_tag[Num_Tags - 1]] = true;
    tag_allocated[lowest_highest_corr_tag[Num_Tags - 2]] = true;
    
    // 给出最佳tag在磁盘上的排列顺序
    int front_tag = lowest_highest_corr_tag[Num_Tags - 1];
    int sequence_pointer = 1;
    while (true) {
        // 为front tag 对面位置分配一个 capacity互补的
        int position;
        bool less_one;
        for (int i = 0; i < Num_Tags / 2; i++) {
            if (tag_less[i] - 1 == front_tag) {
                position = i;
                less_one = true;
                break;
            }
            if (tag_more[i] - 1 == front_tag) {
                position = i;
                less_one = false;
                break;
            }
        }
        int push_ahead_position = position;
        int push_back_position = position;
        if (less_one) {
            while (true) {
                if (!tag_allocated[tag_more[push_ahead_position] - 1] || !tag_allocated[tag_more[push_back_position] - 1]) {
                    break;
                }
                if (tag_allocated[tag_more[push_ahead_position] - 1] && push_ahead_position != Num_Tags / 2 - 1) {
                    push_ahead_position++;
                }
                if (tag_allocated[tag_more[push_back_position] - 1] && push_back_position != 0) {
                    push_back_position--;
                }
            }
            if (!tag_allocated[tag_more[push_ahead_position] - 1]) {
                tag_allocated[tag_more[push_ahead_position] - 1] = true;
                tag_sequence[sequence_pointer] = tag_more[push_ahead_position];
                sequence_pointer++;
                front_tag = tag_more[push_ahead_position] - 1;
            }
            else {
                tag_allocated[tag_more[push_back_position] - 1] = true;
                tag_sequence[sequence_pointer] = tag_more[push_back_position];
                sequence_pointer++;
                front_tag = tag_more[push_back_position] - 1;
            }
        }
        else {
            while (true) {
                if (!tag_allocated[tag_less[push_ahead_position] - 1] || !tag_allocated[tag_less[push_back_position] - 1]) {
                    break;
                }
                if (tag_allocated[tag_less[push_ahead_position] - 1] && push_ahead_position != Num_Tags / 2 - 1) {
                    push_ahead_position++;
                }
                if (tag_allocated[tag_less[push_back_position] - 1] && push_back_position != 0) {
                    push_back_position--;
                }
            }
            if (!tag_allocated[tag_less[push_ahead_position] - 1]) {
                tag_allocated[tag_less[push_ahead_position] - 1] = true;
                tag_sequence[sequence_pointer] = tag_less[push_ahead_position];
                sequence_pointer++;
                front_tag = tag_less[push_ahead_position] - 1;
            }
            else {
                tag_allocated[tag_less[push_back_position] - 1] = true;
                tag_sequence[sequence_pointer] = tag_less[push_back_position];
                sequence_pointer++;
                front_tag = tag_less[push_back_position] - 1;
            }
        }
        // 为front tag 相邻位置分配一个相关性高的
        vector<size_t> corr_sort_idx = sort_indexes(tag_req_correlation[front_tag]);
        for (int i=0;i<Num_Tags;i++){
            if (tag_allocated[corr_sort_idx[i]]) {
                continue;
            }
            tag_allocated[corr_sort_idx[i]] = true;
            tag_sequence[sequence_pointer] = corr_sort_idx[i] + 1;
            sequence_pointer++;
            front_tag = corr_sort_idx[i];
            break;
        }

        if (tag_sequence[sequence_pointer] != 0) {
            break;
        }
    }
    
    // 开始在磁盘上分配分区
    vector<int> init_pt;
    init_pt.resize(Num_Disks);
    for (int i = 0; i < Num_Disks; i++) {
        init_pt[i] = 1;
    }

    for (int d = 0; d < Num_Disks; d++) {
        for (int i = 0; i < Num_Tags; i++) {
            int t = tag_sequence[i];
            for (int m = 0; m < REP_NUM; m++) {
                if (i % 2) {
                    Partition part1 = Partition(t, tag_memory_one_part[t - 1], init_pt[d], true, false);
                    disks[d].partitions.push_back(part1);
                    init_pt[d] += tag_memory_one_part[t - 1];
                }
                else {
                    Partition part1 = Partition(t, tag_memory_one_part[t - 1], init_pt[d], false, false);
                    disks[d].partitions.push_back(part1);
                    init_pt[d] += tag_memory_one_part[t - 1];
                }
            }
        }
    }
    // 归属replica partition 和 unit
    for (int d = 0; d < Num_Disks; d++) {
        for (int t = 0; t < Num_Tags; t++) {
            vector<int> replica_disks;
            vector<int> replica_partitions;
            vector<vector<int>> replica_units;
            replica_disks.resize(REP_NUM);
            replica_partitions.resize(REP_NUM);
            replica_units.resize(REP_NUM);
            for (int r = 0; r < REP_NUM; r++) {
                replica_disks[r] = (d + r) % 10;
                replica_partitions[r] = (t * REP_NUM + r + 1);
                replica_units[r].resize(disks[d].partitions[t * REP_NUM + r + 1].sz);
                for (int u = 0; u < disks[d].partitions[t * REP_NUM + r + 1].sz; u++) {
                    replica_units[r][u] = disks[d].partitions[t * REP_NUM + r + 1].first_unit + u;
                }
            }

            for (int r1 = 0; r1 < REP_NUM; r1++) {
                int rep_num = 0;
                for (int r2 = 0; r2 < REP_NUM; r2++) {
                    if (r1 == r2) {
                        continue;
                    }
                    int d1 = replica_disks[r1];
                    int d2 = replica_disks[r2];
                    int p1 = replica_partitions[r1];
                    int p2 = replica_partitions[r2];
                    disks[d1].partitions[p1].rep_disk_id[rep_num] = d2;
                    disks[d1].partitions[p1].rep_part_id[rep_num] = p2;
                    for (int u = 0; u < disks[d].partitions[t * REP_NUM + 1].sz; u++) {
                        int u1 = replica_units[r1][u];
                        int u2 = replica_units[r2][u];
                        disks[d1].units[u1].partition_id = p1;
                        disks[d1].units[u1].rep_disk_id[rep_num] = d2;
                        disks[d1].units[u1].rep_part_id[rep_num] = p2;
                        disks[d1].units[u1].rep_unit_id[rep_num] = u2;
                    }
                    rep_num++;
                }
            }
        }
    }


    // 剩余内存进行无tag分区

    for (int i = 0; i < Num_Disks; i++) {
        disks[i].partitions[0].first_unit = init_pt[i];
        disks[i].partitions[0].last_unit = SZ_Disks;
        disks[i].partitions[0].init_pointer = init_pt[i];
        disks[i].partitions[0].full = false;
        disks[i].partitions[0].num_req = 0;
        disks[i].partitions[0].unassigned = true;
        disks[i].partitions[0].sz = SZ_Disks - init_pt[i] + 1;
        if (disks[i].partitions[0].sz == 0) {
            disks[i].partitions[0].sz = 1;
        }
        disks[i].partitions[0].reverse_write = false;
        disks[i].partitions[0].reverse_pointer = SZ_Disks;
    }
}

////////////////////// 初始化函数（子函数）初始化自定义全局变量 global_variable_initialize                     
void global_variable_initialize() {
    // 在这里计算require_token 表、 require_token_accumulated 表、max_continuous_read
    int need_token = 64;
    int token_left = Num_Tokens;
    max_continuous_read = 0;
    while (true) {
        require_token.push_back(need_token);
        if (require_token_accumulated.size() == 0) {
            require_token_accumulated.push_back(require_token[max_continuous_read]);
        }
        else {
            require_token_accumulated.push_back(require_token_accumulated[max_continuous_read - 1] + require_token[max_continuous_read]);
        }

        need_token = ceil(double(need_token) * 0.8);
        if (need_token <= 16) {
            need_token = 16;
        }
        if (token_left - require_token[max_continuous_read] >= 0) {
            token_left -= require_token[max_continuous_read];
            max_continuous_read++;
        }
        else {
            break;
        }
    }

    //在这里计算最小pass读取
    token_left = Num_Tokens - 64 - 1 - 64 - 1 - 64 - 1 - 64;
    min_pass_read = 7;
    while (true) {
        token_left -= require_token[min_pass_read - 6];
        min_pass_read++;
        if (token_left < 0) {
            min_pass_read--;
            break;
        }
    }


    // 初始化磁盘头的默认action_flow
    dft_action_flow.resize(Num_Tokens + 1);
    for (int i = 1; i < Num_Tokens + 1; i++) {
        dft_action_flow[i] = "";
        for (int j = 0; j < i; j++) {
            dft_action_flow[i] += "p";
        }
    }
    for (int i = 0; i < Num_Tokens + 1; i++) {
        dft_action_flow[i] += "#\n";
    }

    // 初始化request_loop
    for (int i = 0; i < EXTRA_TIME; i++)
    {
        lose_value_req_loop[i].resize(loop_unit_size);
    }

    //  用于生成随机遍历的磁盘顺序
    disk_sequence.resize(Num_Disks);
    for (int i = 0; i < Num_Disks; i++) {
        disk_sequence[i] = i;
    }

    // 初始化request[0]
    for (int i = 0; i < MAX_OBJECT_SIZE; i++) {
        request[0].have_read[i] = true;
    }
    request[0].is_done = true;

    // 初始化已完成待上报的任务
    finished_requests.resize(50000);

    // 预分配上报任务字符串内存
    output_cache.reserve(1024 * 1024 * 30);

    // 计算不同size的request每个unit值多少分
    request_sz_unit_value.resize(MAX_OBJECT_SIZE);
    request_sz_unit_value_ts_loss.resize(MAX_OBJECT_SIZE);
    for (int i = 0; i < MAX_OBJECT_SIZE; i++) {
        request_sz_unit_value[i] = (0.5 * i + 1) / (i + 1);
        request_sz_unit_value_ts_loss[i] = request_sz_unit_value[i] / EXTRA_TIME;
    }

    // 用于记录tag是否on heat
    tag_on_heat.resize(Num_Tags);
    for (int i = 0; i < Num_Tags; i++) {
        tag_on_heat[i].resize(ceil(double(Num_Tags) / 1800));
    }
}

////////////////////// 初始化函数（母函数）initialize
void initialize()
{
    //读入 T M N V G K
    if (debug_mode) {
        string line;
        getline(file, line);
        stringstream ss(line);
        ss >> Total_TS >> Num_Tags >> Num_Disks >> SZ_Disks >> Num_Tokens >> Num_Exchange;
    }
    else {
        scanf("%d%d%d%d%d", &Total_TS, &Num_Tags, &Num_Disks, &SZ_Disks, &Num_Tokens, &Num_Exchange);
    }

    // 初始化 自定义的全局变量
    global_variable_initialize();

    // 初始化磁盘分区
    disk_partition_initialize();


    printf("OK\n");
    fflush(stdout);
}



int main()
{

    // 执行初始化
    initialize();

    // 循环时间片操作
    for (int t = 1; t <= Total_TS + EXTRA_TIME; t++) {
        // 固定随机种子用于调试
        srand(randseed);
        if (request[1692].have_read[0]) {
            int a = 1;
        }

        if (t == 737) {
            int a = disks[2].pointer_location;
        }
        timestamp_action(); //时间片对齐 + 处理超时的request

        delete_action();//对象删除

        write_action();//对象写入

        read_action();//对象读取

    }
    //clean();

    return 0;
}
