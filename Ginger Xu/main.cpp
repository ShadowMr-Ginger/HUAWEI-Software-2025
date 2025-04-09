
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
#include <queue>

using namespace std;
#pragma endregion

///////////////////////////////////////////////// 本地debug模式设置
ifstream file("sample_practice.txt");
bool debug_mode = false;
int ts = 0;

/////////////////////////////////////////////////////// 算法可调参数
float jump_req_num_rate = 0.5;
double randseed = 666; // 随机种子

double no_tag_rate = 0.01; // 无tag区的size_rate
int urgent_request_ts = 95; // 超过urgent_request_ts仍未被上报的任务，标记紧急上报
int tag_on_heat_period_threshold = 10000; // 判断tag在某一个period是否on_heat 的请求数量阈值
double unfi_req_unit_rate_for_continuous_read = 0.38; // 进行连续读取所需要的任务unit比例
//double threshold_rate_of_reqU_for_continuous_read = 0.38; // 一个part需要连续read的req_u比例
double actual_process_unit_rate = 0.90; // 可以处理的unit占最大可处理unit的比例


// ReadingLoop 部分的可调参数
// 算法外设参数：接受增加part的分数阈值
double accept_add_value_threshold = 30.0;
int accept_add_value_start_ts = 12000;
// 算法外设参数：接受删除part的分数阈值
double accept_cancel_value_threshold = 0;
double accept_exchange_value_threshold = 30;

double total_limit_ts_factor = 85;  //  建议取值105以下，不要超过105，可能报错

////////////////////////a* search部分的可调参数
// 算法外设参数：扫描倍数因子
int scan_mutiple_factor = 10;
// 算法外设参数：取消请求的惩罚因子
double cancellation_penalty_factor = 0.8;
// 算法外设参数：取消请求的门槛
double cancellation_req_ratio_threshold = 1.0;
// 算法外设参数：token向score的转化率
double token_score_transfer_rate = 1.0;
// 算法外设参数：A* search 剪枝系数
double cut_twig_factor = 1.2;
// 算法外设参数：A* search 搜索深度
int search_depth = 40;

/////////////////  前置工具函数
#pragma region ToolFunction
//排序vector返回索引函数
template <typename T>
std::vector<size_t> sort_indexes(const std::vector<T>& v, bool ascending = true) {
    std::vector<size_t> idx(v.size());
    std::iota(idx.begin(), idx.end(), 0);
    if (ascending) {
        std::sort(idx.begin(), idx.end(),
            [&v](size_t i1, size_t i2) { return v[i1] < v[i2]; });
    }
    else {
        std::sort(idx.begin(), idx.end(),
            [&v](size_t i1, size_t i2) { return v[i1] > v[i2]; });
    }
    return idx;
}

template <typename T>
auto sum_vector(const std::vector<T>& v) -> T {
    return std::accumulate(v.begin(), v.end(), T{ 0 });
}

// 返回最大值的索引
template <typename T>
size_t index_of_max(const vector<T>& v) {
    if (v.empty()) throw invalid_argument("Vector is empty");
    return distance(v.begin(), max_element(v.begin(), v.end()));
}

// 返回最小值的索引
template <typename T>
size_t index_of_min(const vector<T>& v) {
    if (v.empty()) throw invalid_argument("Vector is empty");
    return std::distance(v.begin(), min_element(v.begin(), v.end()));
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
#define DISK_HEAD_NUM (2)
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
    int called_times = 0;
    int last_called_ts = -1;
    bool is_delete = false;
    bool is_wasted = false;
    int last_req_ts = -1;
    bool is_urgent = false;
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
    bool is_urgent = false;

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

    //vector<int> occupied_tag_u_number;
    int first_unit = -1;
    int last_unit = -1;
    bool reverse_write = false;
    bool unassigned = true;
    int init_pointer = 0;
    int reverse_pointer = 0;
    bool full = false;
    int num_req = 0;
    double req_rho = 0.0;
    bool is_urgent = false;

    int req_u_number = 0;    //  part存在的未被读取的unit数量
    int accept_status = 2;  // 2 表示可以无限接取  1 表示可在一定范围上接取  0 表示不可接取  3 待定
    int accept_first_u = -1; // 当 accept_status = 1时，第一个可以接取request的unit
    int accept_last_u = -1; // 当 accept_status = 1时，第二个可以接取request的unit

    bool on_reading = false; // 记录该part是否正在被读取
    int finish_reading_surplus_ts = 0; //记录该part还需要几个ts读取完

    vector<int> num_tag_units;

    vector<double> tag_u_avg_req;
    vector<double> score_for_statuses;
    vector<int> process_u_require_for_statuses;
    int occupied_u_number = 0;
    
    double actual_req_u_rate = 0;

    /// <summary>
    /// 最新算法的参数
    /// </summary>
    double num_following_105_ts_requests = 0.0;
    double estimating_ts_for_reading = 0.0;
    double estimating_ts_ratio_for_reading = 0.0;
    double requests_time_ratio = 0.0;
    bool is_chosen = false;
    double num_u_with_req = 0.0;

    bool can_accept_new_req = true;
    /// <summary>
    /// 最新算法的参数
    /// </summary>

    bool on_heat = false; // 该子区是否为被主要读取的对象
    int rep_disk_id[REP_NUM - 1] = { 0 }; // 该单元存储object的其他replica所在磁盘编号
    int rep_part_id[REP_NUM - 1] = { 0 }; // 该单元存储object的其他replica所在磁盘unit号
    Partition() {};
    Partition(int tag_, int sz_, int first_unit_, bool reverse_write_, bool unassigned_) {
        tag = tag_;
        sz = sz_;
        first_unit = first_unit_;
        last_unit = first_unit_ + sz_ - 1;
        accept_first_u = first_unit;
        accept_last_u = last_unit;
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
        num_tag_units.resize(Num_Tags);

        tag_u_avg_req.resize(Num_Tags);
        score_for_statuses.resize(3);
        process_u_require_for_statuses.resize(3);
    }
};

struct ReadingLoop {

    int loop_sz = 0;
    vector<int> part_sequence;
    vector<double> part_num_105ts_req;
    double total_num_105ts_req = 0.0;
    vector<double> part_ts_ratio;
    double total_ts_ratio = 0.0;

};

struct DiskHead {
    int pointer_location = 1; // 磁盘pointer当前所在的unit
    int current_partition = 0; //  记录pointer当前所在磁盘part
    bool execute_jump = false;  // 是否需要执行jump
    int jump_unit = 0;  // jump的目标unit
    string action_flow = ""; // 执行动作流
    /// <用于记录disk指针运动规划的变量>
    vector<int> read_plan; // 记录执行read的位置
    vector<int> read_obj; // 对应read_plan，记录read的object
    vector<int> read_phase; // 记录read_plan结束的位置
    int read_plan_pointer = 0; // 记录一个read_plan当中read的次数
    int plan_over_mark = 0;// 记录action_flow_plan

    int urgency_level = 0; // 记录磁盘头当前执行任务的紧急程度  0为较低（不在 hot part） 1为正常（在 hot part） 2为较高（在 urgent part）

    ////////
    // 新算法部分
    ////////

    ReadingLoop readingloop;
    int current_task_part = 0;//  记录磁盘头当前正在处理的磁盘part
    bool mission_started = false;
    bool cancel_current_part = false;
    bool exchange_current_part = false;
    int exchange_part = 0;

    bool  currently_free = true;
    ////////
    // 新算法部分
    ////////

    /// </用于记录disk指针运动规划的变量>
    DiskHead(int max_continuous_read) {
        action_flow.reserve(Num_Tokens + 2);
        read_plan.resize(max_continuous_read * DISK_HEAD_NUM);
        read_obj.resize(max_continuous_read * DISK_HEAD_NUM);
        read_phase.resize(max_continuous_read * DISK_HEAD_NUM);
    }
};

struct Disk {
    //磁盘
    vector<Partition> partitions; // 分区

    vector<Unit> units; // 单元

    vector<DiskHead> diskheads;

    vector<bool> part_activated;

    int exchange_times = 0;

    int num_spare_units = 0; //空闲unit数量

    Disk() {};
    Disk(int capacity, int max_continuous_read) {
        partitions.resize(1);
        units.resize(capacity + 1);
        num_spare_units = capacity;

        for (int i = 1; i < capacity; i++) {
            units[i].nxt_unit_id = i + 1;
        }
        units[capacity].nxt_unit_id = 1;
        units[0].occupied = true;

        for (int i = 0; i < DISK_HEAD_NUM; i++) {
            DiskHead dh = DiskHead(max_continuous_read);
            diskheads.push_back(dh);
        }
    }
};

struct Tag_To_Partition {
    // 用于记录一个tag存储于哪些partition
    //vector<int> disk_list;
    vector<int> partition_list;
    vector<int> over_write_partition_list;
};

struct Partition_Evaluator {
    int d;
    int p;
    vector<double> score_for_statuses;
    vector<int> process_u_require_for_statuses;

    Partition_Evaluator(int d_, int p_) {
        d = d_;
        p = p_;
        score_for_statuses.resize(3);
        process_u_require_for_statuses.resize(3);
    }
};


struct Score_Range {
    int start_u = 0;
    int len = 0;
    double score_per_u = 0;
    int distance_from_last_range = 0;
    //bool lower_than_avg = false;
};





#pragma endregion

////////////////// 全局状态变量
#pragma region Global_StatusVar_and_Definition

/////////////////  新增全局变量


vector<Disk> disks;
vector<Tag_To_Partition> tag_to_partition;
vector<Partition_Evaluator> partition_evaluator;
vector<int> require_token; // 记录连续读取的token消耗
vector<int> require_token_accumulated;
int max_continuous_read = 0;
int min_pass_read = 0;
double can_read_unit_per_token = 0.0;

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

// 用于记录输出繁忙的任务
vector<int> busy_requests;
int n_busy = 0;

// 用于记录每个period，每个tag的读取是否on_heat
vector<vector<bool>> tag_on_heat;

// 用来记录urgent_req
int urgent_loop_pt = -1;
vector<vector<int>> urgent_req_loop;
vector<int> urgent_req_pt;

vector<int>urgent_obj_list;
int urgent_obj_list_pt = 0;

// 用来记录tag在当前ts是否on heat
vector<bool> currently_tag_on_heat;
vector<bool> currently_tag_ignore;
int max_can_process_unit_in_time = 0;
vector<int> currently_tag_unfi_req;

// 记录每个disk有多少part是原始非replica的part
int num_total_tag_part;

// 用来记录tag在150ts内的请求总量变化值  用来计算tag req 趋势
vector<vector<int>> last_150_ts_tag_req;
int current_pt = 0;
int last_50_pt = 100;
int last_100_pt = 50;
vector<double> avg_last_0_50_req;
vector<double> avg_last_50_100_req;
vector<double> avg_last_100_150_req;
vector<double> current_avg_ts_req_slope;
vector<double> next_105_tag_req;
vector<double> next_105_tag_req_per_unit;
vector<int> num_tag_unit;
vector<int> num_tag_object;
vector<double> tag_u_avg_req_next_105_ts;


struct Node {
    int current_step = 0; // 当前步
    int current_step_read_u = 0;
    int last_action = -1; // 取值0代表上一步是 pass and cancel，取值1代表上一步是 pass and read，取值2代表上一步是continuous read
    int surplus_token = 0; // 当前剩余token
    int current_continuous_read_num = 0;
    vector<int> history_actions;
    double current_score = 0.0;
    double actual_score = 0.0;
    bool endnode = false;
    Node() {
        history_actions.reserve(100);
    }
    Node(int _surplus_token) {
        surplus_token = _surplus_token;
    }
    //下面这个重载，用于直接计算next node 的score
    Node(Node last_node, int current_action, vector<Score_Range> score_ranges, int d, double avg_req_u) {
        history_actions.reserve(100);
        endnode = false;
        int s = last_node.current_step;
        surplus_token = last_node.surplus_token;
        current_continuous_read_num = last_node.current_continuous_read_num;
        if (current_action == 2) {
            // 先c_r抵达该step的第一个u
            for (int i = 0; i < score_ranges[s].distance_from_last_range; i++) {
                if (surplus_token - require_token[current_continuous_read_num] >= 0) {
                    surplus_token -= require_token[current_continuous_read_num];
                    current_continuous_read_num++;
                }
                else {
                    endnode = true;
                    break;
                }
            }
            if (!endnode) {
                if (surplus_token - require_token[current_continuous_read_num] < 0) {
                    endnode = true;
                }
            }
        }else if (current_action == 1){
            // 先pass抵达该step的第一个u
            if (score_ranges[s].distance_from_last_range != 0) {
                current_continuous_read_num = 0;
                if (surplus_token < score_ranges[s].distance_from_last_range) {
                    endnode = true;
                }
                else {
                    surplus_token -= score_ranges[s].distance_from_last_range;
                }
            }
            if (!endnode) {
                if (surplus_token - require_token[current_continuous_read_num] < 0) {
                    endnode = true;
                }
            }
        }
        
        
        if (!endnode) {
            current_step = last_node.current_step + 1;
            last_action = current_action;
            history_actions = last_node.history_actions;
            history_actions.push_back(last_node.last_action);
            
            actual_score = last_node.actual_score;
            current_step_read_u = 0;


            // 首先考虑下一步为 continuous_read

            if (current_action == 2) {
                // 然后c_r 该step
                int current_step_u_num = score_ranges[s].len;
                int u = score_ranges[s].start_u;
                for (int i = 0; i < current_step_u_num; i++) {
                    if (surplus_token - require_token[current_continuous_read_num] >= 0) {
                        surplus_token -= require_token[current_continuous_read_num];
                        current_continuous_read_num++;
                        actual_score += disks[d].units[u].number_unfi_req;
                        current_step_read_u++;
                    }
                    else {
                        endnode = true;
                        break;
                    }
                }
                current_score = actual_score + avg_req_u * can_read_unit_per_token * surplus_token * token_score_transfer_rate;
            }
            //  接下来考虑下一步为 pass_read
            else if (current_action == 1) {
                // 然后c_r 该step
                int current_step_u_num = score_ranges[s].len;
                int u = score_ranges[s].start_u;
                for (int i = 0; i < current_step_u_num; i++) {
                    if (surplus_token - require_token[current_continuous_read_num] >= 0) {
                        surplus_token -= require_token[current_continuous_read_num];
                        current_continuous_read_num++;
                        actual_score += disks[d].units[u].number_unfi_req;
                        current_step_read_u++;
                    }
                    else {
                        endnode = true;
                        break;
                    }
                }
                current_score = actual_score + avg_req_u * can_read_unit_per_token * surplus_token * token_score_transfer_rate;
            }
            // 最后考虑下一步为cancel
            else {
                // 先pass抵达该step的第一个u
                if (score_ranges[s].distance_from_last_range != 0) {
                    current_continuous_read_num = 0;
                    if (surplus_token < score_ranges[s].distance_from_last_range) {
                        endnode = true;
                    }
                    else {
                        surplus_token -= score_ranges[s].distance_from_last_range;
                    }
                }
                // 然后计算cancel降分
                if (!endnode) {
                    int current_step_u_num = score_ranges[s].len;
                    int u = score_ranges[s].start_u;
                    current_continuous_read_num = 0;
                    for (int i = 0; i < current_step_u_num; i++) {
                        if (surplus_token - 1 >= 0) {
                            surplus_token--;
                            actual_score -= disks[d].units[u].number_unfi_req * cancellation_penalty_factor; // 是否需要进一步深思熟虑？？？？？？？？？？？？？？？？
                            current_step_read_u++;
                        }
                        else {
                            endnode = true;
                            break;
                        }
                    }
                }
                current_score = actual_score + avg_req_u * can_read_unit_per_token * surplus_token * token_score_transfer_rate;
            }
            if (s + 1 == score_ranges.size()) {
                endnode = true;
            }
        }
        else {
            current_continuous_read_num = last_node.current_continuous_read_num;
            current_step = last_node.current_step;
            last_action = last_node.last_action;
            history_actions = last_node.history_actions;
            surplus_token = last_node.surplus_token;
            actual_score = last_node.actual_score;
        }
    }
};

// 比较器：分数高的优先
struct CompareNode {
    bool operator()(const Node& a, const Node& b) const {
        return a.current_score < b.current_score;
    }
};


#pragma endregion

///////////////////////////////////////////////////////////////////    函数部分    /////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////    函数部分    /////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////    函数部分    /////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////    函数部分    /////////////////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////// RECYCLE ////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

//
void do_void_filling() {

    for (int d = 0; d < Num_Disks; d++) {
        disks[d].exchange_times = Num_Exchange;
    }

    random_shuffle(disk_sequence.begin(), disk_sequence.end());
    for (int c = 0; c < Num_Disks; c++) {
        int d = disk_sequence[c];
        int exchange_left = disks[d].exchange_times;
        // 检查每个part的void区
        
    }
}

//////////////////  垃圾回收操作 （母函数）  执行unit交换
void garbage_collection_action() {
    string line;
    if (debug_mode) {
        getline(file, line);
        if (line[0] != 'G') {
            throw runtime_error("行错误，请检查");
        }
        printf("%s\n", line.c_str());
    }
    else {
        char line1[100];
        char line2[100];
        scanf("%s %s", line1,line2);
        printf("GARBAGE COLLECTION\n");
    }

    output_cache.clear();
    for (int i = 0; i < Num_Disks; i++) {
        output_cache += "0\n";
    }
    printf(output_cache.c_str());

    fflush(stdout);

    
    //output_cache += "GARBAGE COLLECTION\n";

    //printf(output_cache.c_str());
    
}

//////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////// READ //////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////


//////////////////  对象读取操作 （子函数）  执行磁盘指针动作的输出 
void print_pointer_action_and_finished_requsets() {

    // 上报完成的请求
    output_cache.clear();
    for (int d = 0; d < Num_Disks; d++) {
        for (int h = 0; h < DISK_HEAD_NUM; h++) {
            if (disks[d].diskheads[h].execute_jump) {
                output_cache += "j " + to_string(disks[d].diskheads[h].jump_unit) + "\n";
                disks[d].diskheads[h].execute_jump = false;
            }
            else {
                output_cache += disks[d].diskheads[h].action_flow;
            }
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

    ////////////////// 上报繁忙的请求

    output_cache.clear();
    output_cache += to_string(n_busy) + "\n";
    for (int i = 0; i < n_busy; i++) {
        output_cache += to_string(busy_requests[i]);  // 拼接字符串或数字
        output_cache += "\n";
    }
    printf(output_cache.c_str());
    n_busy = 0;
    fflush(stdout);
}

void update_busy_request_status() {
    
    int minus_n_busy = 0;
    for (int q = 0; q < n_busy; q++) {
        int req_id = busy_requests[q];
        if (!request[req_id].is_done) {
            request[req_id].is_done = true;
            int obj_id = request[req_id].object_id;
            int sz = object[obj_id].size;
            for (int r = 0; r < REP_NUM; r++) {
                int d = object[obj_id].replica_disk[r];
                int p = object[obj_id].replica_partition[r];
                for (int v = 0; v < sz; v++) {
                    if (request[req_id].have_read[v]) {
                        continue;
                    }
                    int u = object[obj_id].store_units[r][v];
                    disks[d].units[u].number_unfi_req--;
                    disks[d].partitions[p].num_req--;
                }
            }
        }
        else {
            for (int i = q; i < n_busy - 1; i++) {
                busy_requests[i] = busy_requests[i + 1];
            }
            q--;
            n_busy--;
        }
    }

    // 临终检查
    for (int d = 0; d < Num_Disks; d++) {
        for (int p = 1; p <= num_total_tag_part; p++) {
            if (disks[d].partitions[p].accept_status == 0 && disks[d].partitions[p].num_req > 0) {
                int first_u = disks[d].partitions[p].first_unit;
                int last_u = disks[d].partitions[p].last_unit;
                for (int u = first_u; u <= last_u; u++) {
                    if (disks[d].units[u].number_unfi_req > 0) {
                        int obj_id = disks[d].units[u].obj_id;
                        int last_req = object[obj_id].last_request_point;
                        int sz = object[obj_id].size;
                        while (!request[last_req].is_done) {
                            request[last_req].is_done = true;
                            busy_requests[n_busy] = last_req;
                            n_busy++;
                            for (int r = 0; r < REP_NUM; r++) {
                                int dd = object[obj_id].replica_disk[r];
                                int pp = object[obj_id].replica_partition[r];
                                for (int v = 0; v < sz; v++) {
                                    if (request[last_req].have_read[v]) {
                                        continue;
                                    }
                                    int uu = object[obj_id].store_units[r][v];
                                    disks[dd].units[uu].number_unfi_req--;
                                    if (disks[dd].units[uu].number_unfi_req < 0) {
                                        throw runtime_error("unit请求数量异常");
                                    }
                                    disks[dd].partitions[pp].num_req--;
                                }
                            }
                            last_req = request[last_req].prev_id;
                        }
                    }
                }
            }
        }
    }
}

//////////////////  对象读取操作（子函数的子函数） object、request状态更新 
void update_request_status(int d, int h) {
    // 更新被读取的object状态和request状态
    int obj_id = disks[d].diskheads[h].read_obj[0];
    vector<int> read_phase;
    read_phase.resize(MAX_OBJECT_SIZE);
    int iter = 0;
    read_phase[iter] = disks[d].diskheads[h].read_phase[0];
    iter++;
    for (int i = 1; i <= disks[d].diskheads[h].read_plan_pointer; i++) {
        if (i == disks[d].diskheads[h].read_plan_pointer || (disks[d].diskheads[h].read_obj[i] != obj_id && disks[d].diskheads[h].read_obj[i] != 0 && obj_id != 0)) {
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
            int last_req = object[obj_id].last_request_point;
            // 逐一校验任务
            
            while (true) {
                if (request[last_req].is_done) {
                    break;
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

            if (i != disks[d].diskheads[h].read_plan_pointer) {
                obj_id = disks[d].diskheads[h].read_obj[i];
                read_phase[iter] = disks[d].diskheads[h].read_phase[i];
                iter++;
            }
        }
        else {
            if (obj_id != 0 && disks[d].diskheads[h].read_obj[i] != 0) {
                read_phase[iter] = disks[d].diskheads[h].read_phase[i];
                iter++;
            }
            if (obj_id == 0 && disks[d].diskheads[h].read_obj[i] != 0) {
                iter = 0;
                obj_id = disks[d].diskheads[h].read_obj[i];
                read_phase[iter] = disks[d].diskheads[h].read_phase[i];
                iter++;
            }
        }
    }

    disks[d].diskheads[h].read_plan_pointer = 0;
}

////////////////////  对象读取操作（子函数的子函数） 执行磁盘头规划（非连续读取）
void excute_pass_read(int d, int h, int token_left, int num_actions, int continuous_read) {
    // 不考虑连续读取，直接往前读取带有request的unit
    int pt = disks[d].diskheads[h].pointer_location;
    while (token_left - require_token[continuous_read] >= 0) {
        if (disks[d].units[pt].number_unfi_req > 0 && disks[d].units[pt].occupied) {
            // read 一次
            token_left -= require_token[continuous_read];  //扣除token_left
            num_actions++;  // 增加action数
            continuous_read++; // 增加连续读取数
            disks[d].diskheads[h].read_plan[disks[d].diskheads[h].read_plan_pointer] = num_actions; // 更新action flow的read标记
            disks[d].diskheads[h].read_obj[disks[d].diskheads[h].read_plan_pointer] = disks[d].units[pt].obj_id;
            disks[d].diskheads[h].read_phase[disks[d].diskheads[h].read_plan_pointer] = disks[d].units[pt].obj_phase;
            disks[d].diskheads[h].read_plan_pointer++;

            int fi_req = disks[d].units[pt].number_unfi_req;
            int p = disks[d].units[pt].partition_id;
            if (fi_req > 0) {
                disks[d].partitions[p].req_u_number--;
            }
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

    //// 赋予该ts的指令
    disks[d].diskheads[h].plan_over_mark = num_actions;
    disks[d].diskheads[h].action_flow = dft_action_flow[num_actions];
    for (int t = 0; t < disks[d].diskheads[h].read_plan_pointer; t++) {
        disks[d].diskheads[h].action_flow[disks[d].diskheads[h].read_plan[t] - 1] = 'r';
    }

    ////////////////////////////
    disks[d].diskheads[h].pointer_location = pt;
    // 更新request状态
    if (disks[d].diskheads[h].read_plan_pointer > 0) {
        update_request_status(d, h);
    }
    //////////////////////////

}

//////////////////  对象读取操作（子函数的子函数） 计算可以连续读取的block数量
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
void a_star_search_diskhead_actionflow(int d, int h, int start_pt, int surplus_token, int num_actions) {

    // 算法主体
    // STEP 1 : 向前扫描整个 scan_mutiple_factor * max_continuous_read 范围
    vector<Score_Range> score_ranges;
    double avg_req_u = 0.0;
    for (int u = start_pt; u < start_pt + scan_mutiple_factor * max_continuous_read; u++) {
        avg_req_u += disks[d].units[u].number_unfi_req;
    }
    avg_req_u /= scan_mutiple_factor * max_continuous_read;
    Score_Range sc;
    int dist = 0;
    for (int u = start_pt; u < start_pt + scan_mutiple_factor * max_continuous_read; u++) {
        if (!disks[d].units[u].occupied || disks[d].units[u].number_unfi_req == 0) {
            dist++;
            if (sc.len > 0) {
                sc.score_per_u /= sc.len;
                score_ranges.push_back(sc);
                sc.len = 0;
            }
        }
        else {
            if (sc.len == 0) {
                sc.start_u = u;
                sc.len = 1;
                sc.score_per_u = disks[d].units[u].number_unfi_req;
                //if (disks[d].units[u].number_unfi_req < avg_req_u) {
                //    sc.lower_than_avg = true;
                //}
                //else {
                //    sc.lower_than_avg = false;
                //}
                sc.distance_from_last_range = dist;
                dist = 0;
            }
            else {
                //if (disks[d].units[u].number_unfi_req >= avg_req_u && sc.lower_than_avg) {
                //    sc.score_per_u /= sc.len;
                //    score_ranges.push_back(sc);
                //    sc.start_u = u;
                //    sc.len = 1;
                //    sc.score_per_u = disks[d].units[u].number_unfi_req;
                //    sc.lower_than_avg = false;
                //    sc.distance_from_last_range = dist;
                //}
                //else if (disks[d].units[u].number_unfi_req < avg_req_u && !sc.lower_than_avg) {
                //    sc.score_per_u /= sc.len;
                //    score_ranges.push_back(sc);
                //    sc.start_u = u;
                //    sc.len = 1;
                //    sc.score_per_u = disks[d].units[u].number_unfi_req;
                //    sc.lower_than_avg = true;
                //    sc.distance_from_last_range = dist;
                //}
                //else {
                sc.len++;
                sc.score_per_u += disks[d].units[u].number_unfi_req;
                //}
            }
        }
    }
    if (sc.len > 0) {
        sc.score_per_u /= sc.len;
        score_ranges.push_back(sc);
        sc.len = 0;
    }

    if (ts == 3016 && d==1&& h==0) {
        int a = 1;
    }

    // STEP 2 : A* search法寻找最佳方案
    // 创建优先队列
    priority_queue<Node, vector<Node>, CompareNode> node_queue;

    // 给出第一个节点
    Node init_node = Node(surplus_token);
    node_queue.push(init_node);

    // 保存最佳节点
    Node best_node = init_node;

    while (true) {

        // 取出优先队列顶部的节点
        Node node = node_queue.top();
        node_queue.pop();
        // 若为endnode，不执行新增节点。
        if (node.endnode) {
            // 记录该结果
            if (node.actual_score > best_node.actual_score) {
                best_node = node;
            }
        }
        else {
            int s = node.current_step;
            // 首先考虑下一步为 continuous_read
            Node c_r_node = Node(node, 2, score_ranges, d, avg_req_u);
            node_queue.push(c_r_node);
            //  接下来考虑下一步为 pass_read。若distance=0，不考虑pass_read;
            if (score_ranges[s].distance_from_last_range != 0) {
                Node p_r_node = Node(node, 1, score_ranges, d, avg_req_u);
                node_queue.push(p_r_node);
            }
            // 最后考虑下一步为 cancel。若score_range中unit不低于平均req密度，不考虑cancel。
            //if (score_ranges[s].lower_than_avg) {
            //    Node cancel_node = Node(node, 0, score_ranges, d, avg_req_u);
            //    node_queue.push(cancel_node);
            //}
        }

        //给出循环终止的判断条件;
        if (node_queue.empty()) {
            break;
        }
        if (node_queue.top().current_score * cut_twig_factor < best_node.actual_score) {
            break;
        }
        if (node_queue.top().current_step >= search_depth) {
            break;
        }
    }

    // STEP 3 : 后处理搜索最佳结果
    if (best_node.actual_score > 0) {
        disks[d].diskheads[h].read_plan_pointer = 0;
        vector<int> actions = best_node.history_actions;
        actions.erase(actions.begin());
        actions.push_back(best_node.last_action);
        int u = start_pt;
        int continuous_read = 0;
        for (int i = 0; i < actions.size(); i++) {
            if (actions[i] == 0) {
                //这里需要对request进行删除操作
                continuous_read = 0;
                //while (u != score_ranges[i].start_u) {
                //    surplus_token--;
                //    u = disks[d].units[u].nxt_unit_id;
                //    num_actions++;
                //}
                //// 对这个区间的请求上报取消
                //for (int j = 0; j < score_ranges[i].len; j++) {
                //    surplus_token--;
                //    int obj = disks[d].units[u].obj_id;
                //    int phase = disks[d].units[u].obj_phase;
                //    int last_req = object[obj].last_request_point;

                //    仔细考虑上报取消怎么写。;
                //    num_actions++;
                //}

            }
            else if (actions[i] == 1) {
                // 这里需要对request执行pass read
                while (u != score_ranges[i].start_u) {
                    surplus_token--;
                    u = disks[d].units[u].nxt_unit_id;
                    num_actions++;
                    continuous_read = 0;
                }

                for (int j = 0; j < score_ranges[i].len; j++) {
                    if (surplus_token - require_token[continuous_read] >= 0) {
                        surplus_token -= require_token[continuous_read];
                        continuous_read++;
                        num_actions++;
                        int read_plan_pt = disks[d].diskheads[h].read_plan_pointer;
                        int obj = disks[d].units[u].obj_id;
                        int phase = disks[d].units[u].obj_phase;
                        disks[d].diskheads[h].read_obj[read_plan_pt] = obj;
                        disks[d].diskheads[h].read_phase[read_plan_pt] = phase;
                        disks[d].diskheads[h].read_plan[read_plan_pt] = num_actions;
                        disks[d].diskheads[h].read_plan_pointer++;
                        

                        int fi_req = disks[d].units[u].number_unfi_req;
                        int p = disks[d].units[u].partition_id;
                        if (fi_req > 0) {
                            disks[d].partitions[p].req_u_number--;
                        }
                        disks[d].partitions[disks[d].units[u].partition_id].num_req -= fi_req;//清空unit其中未读取的任务
                        disks[d].units[u].number_unfi_req = 0; //清空unit其中未读取的任务
                        for (int k = 0; k < REP_NUM - 1; k++) {//清空 replica 的 unit其中未读取的任务
                            int d2 = disks[d].units[u].rep_disk_id[k];
                            int u2 = disks[d].units[u].rep_unit_id[k];
                            disks[d2].partitions[disks[d2].units[u2].partition_id].num_req -= fi_req;
                            disks[d2].units[u2].number_unfi_req = 0;
                        }
                        u = disks[d].units[u].nxt_unit_id;
                    }
                    else {
                        break;
                    }
                }
            }
            else if (actions[i] == 2) {
                // 这里需要对request执行 continuous read
                while (u != score_ranges[i].start_u) {
                    surplus_token -= require_token[continuous_read];
                    continuous_read++;
                    num_actions++;
                    int read_plan_pt = disks[d].diskheads[h].read_plan_pointer;
                    disks[d].diskheads[h].read_obj[read_plan_pt] = 0;
                    disks[d].diskheads[h].read_phase[read_plan_pt] = 0;
                    disks[d].diskheads[h].read_plan[read_plan_pt] = num_actions;
                    disks[d].diskheads[h].read_plan_pointer++;
                    u = disks[d].units[u].nxt_unit_id;
                }

                for (int j = 0; j < score_ranges[i].len; j++) {
                    if (surplus_token - require_token[continuous_read] >= 0) {
                        surplus_token -= require_token[continuous_read];
                        num_actions++;
                        continuous_read++;
                        int read_plan_pt = disks[d].diskheads[h].read_plan_pointer;
                        int obj = disks[d].units[u].obj_id;
                        int phase = disks[d].units[u].obj_phase;
                        disks[d].diskheads[h].read_obj[read_plan_pt] = obj;
                        disks[d].diskheads[h].read_phase[read_plan_pt] = phase;
                        disks[d].diskheads[h].read_plan[read_plan_pt] = num_actions;
                        disks[d].diskheads[h].read_plan_pointer++;
                        


                        int fi_req = disks[d].units[u].number_unfi_req;
                        int p = disks[d].units[u].partition_id;
                        if (fi_req > 0) {
                            disks[d].partitions[p].req_u_number--;
                        }
                        disks[d].partitions[disks[d].units[u].partition_id].num_req -= fi_req;//清空unit其中未读取的任务
                        disks[d].units[u].number_unfi_req = 0; //清空unit其中未读取的任务
                        for (int k = 0; k < REP_NUM - 1; k++) {//清空 replica 的 unit其中未读取的任务
                            int d2 = disks[d].units[u].rep_disk_id[k];
                            int u2 = disks[d].units[u].rep_unit_id[k];
                            disks[d2].partitions[disks[d2].units[u2].partition_id].num_req -= fi_req;
                            disks[d2].units[u2].number_unfi_req = 0;
                        }
                        u = disks[d].units[u].nxt_unit_id;
                    }
                    else {
                        break;
                    }
                }
            }
            else {
                throw runtime_error("无第四种情况，规划异常");
            }
        }
        if (surplus_token < 0) {
            throw runtime_error("diskhead规划异常，剩余token<0");
        }


        disks[d].diskheads[h].pointer_location = u;
        excute_pass_read(d, h, surplus_token, num_actions, continuous_read);

        ////// 赋予该ts的指令
        //disks[d].diskheads[h].plan_over_mark = num_actions;
        //disks[d].diskheads[h].action_flow = dft_action_flow[num_actions];
        //for (int t = 0; t < disks[d].diskheads[h].read_plan_pointer; t++) {
        //    disks[d].diskheads[h].action_flow[disks[d].diskheads[h].read_plan[t] - 1] = 'r';
        //}

        //////////////////////////////
        //
        //// 更新request状态
        //if (disks[d].diskheads[h].read_plan_pointer > 0) {
        //    update_request_status(d, h);
        //}
        //////////////////////////
    }
    else {
        disks[d].diskheads[h].action_flow = "#\n";
    }

    


    //int last_pt;
    //int pt = disks[d].pointer_location[0];
    //while (true) {
    //    if (start_pt == pt) {
    //        break;
    //    }
    //    if (start_pt == 1) {
    //        last_pt = SZ_Disks;
    //    }
    //    else {
    //        last_pt = start_pt - 1;
    //    }
    //    if (disks[d].units[start_pt].obj_phase != 0 && disks[d].units[last_pt].number_unfi_req != 0) {
    //        start_pt--;
    //        if (start_pt == 0) {
    //            start_pt = SZ_Disks;
    //        }
    //    }
    //    else {
    //        break;
    //    }
    //}
    //// 从start_pt开始检查接下来有请求的request数量
    //int num_actions = 0;
    //int token_left = Num_Tokens;
    //while (pt != start_pt) {
    //    num_actions++;
    //    pt++;
    //    if (pt > SZ_Disks) {
    //        pt = 1;
    //    }
    //    token_left--;
    //}
    ////disks[d].pointer_location = pt;

    //int still_can_continuous_read = calc_max_continuous_read(token_left);
    //int least_num_unfi_req_unit = still_can_continuous_read * 1 + 1;
    //int num_unfi_req_unit = 0;
    //for (int v = 0; v < still_can_continuous_read; v++) {
    //    int u = v + pt;
    //    if (u >= SZ_Disks) {
    //        u -= SZ_Disks;
    //    }
    //    if (disks[d].units[u].number_unfi_req > 0) {
    //        num_unfi_req_unit++;
    //    }
    //}
    //if (num_unfi_req_unit >= least_num_unfi_req_unit) {
    //    // continuous read
    //    execute_continuous_read(d, token_left, pt, num_actions);
    //}
    //else {
    //    // pass read
    //    execute_pass_read(d, token_left, pt, num_actions);
    //}
}



void execute_cycle_part_to_loop(ReadingLoop* current_loop) {

    (*current_loop).part_num_105ts_req.push_back((*current_loop).part_num_105ts_req[0]);
    (*current_loop).part_ts_ratio.push_back((*current_loop).part_ts_ratio[0]);
    (*current_loop).part_sequence.push_back((*current_loop).part_sequence[0]);
    (*current_loop).part_num_105ts_req.erase((*current_loop).part_num_105ts_req.begin());
    (*current_loop).part_ts_ratio.erase((*current_loop).part_ts_ratio.begin());
    (*current_loop).part_sequence.erase((*current_loop).part_sequence.begin());
}

void execute_cancel_part_to_loop(ReadingLoop* current_loop, int d, int p) {
    (*current_loop).total_num_105ts_req -= (*current_loop).part_num_105ts_req[0];
    (*current_loop).total_ts_ratio -= (*current_loop).part_ts_ratio[0];
    (*current_loop).part_num_105ts_req.erase((*current_loop).part_num_105ts_req.begin());
    (*current_loop).part_ts_ratio.erase((*current_loop).part_ts_ratio.begin());
    (*current_loop).part_sequence.erase((*current_loop).part_sequence.begin());
    (*current_loop).loop_sz--;
    disks[d].part_activated[p] = false; 
    disks[d].partitions[p].accept_status = 0;
}

void execute_add_part_to_loop(ReadingLoop* current_loop, int d, int p) {
    (*current_loop).part_sequence.push_back(p);
    double plus_num_req = disks[d].partitions[p].num_following_105_ts_requests;
    (*current_loop).part_num_105ts_req.push_back(plus_num_req);
    (*current_loop).total_num_105ts_req += plus_num_req;
    double plus_reading_ts_ratio = disks[d].partitions[p].estimating_ts_ratio_for_reading;
    (*current_loop).part_ts_ratio.push_back(plus_reading_ts_ratio);
    (*current_loop).total_ts_ratio += plus_reading_ts_ratio;
    (*current_loop).loop_sz++;
    disks[d].part_activated[p] = true;
    disks[d].partitions[p].accept_status = 2;
}

void concider_exchange_part_to_loop(ReadingLoop current_loop, int d, bool* can_exchange, int* exchange_part, vector<size_t> part_iter_sequence) {
    double minus_ts_ratio = current_loop.part_ts_ratio[0];
    for (int i = 0; i < num_total_tag_part; i++) {
        int p = part_iter_sequence[i];
        if (disks[d].part_activated[p]) {
            continue;
        }
        double plus_ts_ratio = disks[d].partitions[p].estimating_ts_ratio_for_reading;
        if (current_loop.total_ts_ratio - minus_ts_ratio+plus_ts_ratio>=1){
            continue;
        }
        double lose_value = current_loop.part_num_105ts_req[0] * (1 - current_loop.total_ts_ratio / 2);
        double gain_value = (current_loop.total_num_105ts_req - current_loop.part_num_105ts_req[0]) * current_loop.part_ts_ratio[0] / 2;

        lose_value += current_loop.total_num_105ts_req * plus_ts_ratio / 2;
        gain_value += disks[d].partitions[p].num_following_105_ts_requests * (1 - (plus_ts_ratio + current_loop.total_ts_ratio) / 2);

        double current_accept_exchange_value_threshold = 0.0;
        if (ts >= accept_add_value_start_ts) {
            current_accept_exchange_value_threshold = accept_add_value_threshold;
        }

        if (gain_value - lose_value > current_accept_exchange_value_threshold) {
            *can_exchange = true;
            *exchange_part = p;
            break;
        }
    }
}

void concider_cancel_part_to_loop(ReadingLoop current_loop, bool* can_cancel) {
    //判断是否增加分数
    double minus_ts_ratio = current_loop.part_ts_ratio[0];
    double lose_value = current_loop.part_num_105ts_req[0] * (1 - current_loop.total_ts_ratio / 2);
    double gain_value = (current_loop.total_num_105ts_req - current_loop.part_num_105ts_req[0]) * current_loop.part_ts_ratio[0] / 2;
    if (gain_value - lose_value > accept_cancel_value_threshold) {
        *can_cancel = true;
    }
    else {
        *can_cancel = false;
    }
}

void concider_add_part_to_loop(ReadingLoop current_loop, Partition part, bool* can_add, double* add_plus_value) {
    if (current_loop.total_ts_ratio + part.estimating_ts_ratio_for_reading < 1) {
        //判断是否增加分数
        double plus_ts_ratio = part.estimating_ts_ratio_for_reading;
        double lose_value = current_loop.total_num_105ts_req * plus_ts_ratio / 2;
        double gain_value = part.num_following_105_ts_requests * (1 - (plus_ts_ratio + current_loop.total_ts_ratio) / 2);
        // gain_value 大于 lose value 超过一定分数，则加入
        double current_accept_add_value_threshold = 0.0;
        if (ts >= accept_add_value_start_ts) {
            current_accept_add_value_threshold = accept_add_value_threshold;
        }
        if (gain_value - lose_value > current_accept_add_value_threshold) {
            *can_add = true;
            *add_plus_value = gain_value - lose_value;
        }
        else {
            *can_add = false;
        }
    }
    else {
        *can_add = false;
    }
}



//////////////////  对象读取操作（子函数） 规划磁盘指针动作
void plan_disk_pointer_loop() {

    for (int d = 0; d < Num_Disks; d++) {
        // 考虑在loop中新增part
        vector<double> part_req_u_rho;
        part_req_u_rho.resize(num_total_tag_part);
        for (int p = 1; p <= num_total_tag_part; p++) {
            part_req_u_rho[p - 1] = disks[d].partitions[p].requests_time_ratio;
        }
        vector<size_t> part_iter_sequence = sort_indexes(part_req_u_rho, false);
        for (int p = 0; p < num_total_tag_part; p++) {
            part_iter_sequence[p]++;
        }


        for (int q = 0; q < num_total_tag_part; q++) {
            int p = part_iter_sequence[q];
            if (disks[d].part_activated[p]) {
                continue;
            }
            int diskhead_sequence[2] = { 0,1 };
            if (disks[d].diskheads[0].readingloop.total_ts_ratio > disks[d].diskheads[1].readingloop.total_ts_ratio) {
                diskhead_sequence[0] = 1;
                diskhead_sequence[1] = 0;
            }
            if (disks[d].diskheads[0].readingloop.loop_sz == 0) {
                ///// 初次新增part到loop中
                execute_add_part_to_loop(&disks[d].diskheads[0].readingloop, d, p);
                continue;
            }

            bool can_add = false;
            int add_h = -1;
            double add_plus_value = 0.0;
            for (int i = 0; i < 2; i++) {
                int h = diskhead_sequence[i];

                bool can_add1 = false;
                double add_plus_value1 = 0.0;
                concider_add_part_to_loop(disks[d].diskheads[h].readingloop, disks[d].partitions[p], &can_add1, &add_plus_value1);
                if (can_add1 && add_plus_value1>add_plus_value) {
                    can_add = true;
                    add_h = h;
                    add_plus_value = add_plus_value1;
                }
            }
            if (can_add) {
                if (disks[d].partitions[p].accept_status == 1) {
                    int a = 1;
                }


                execute_add_part_to_loop(&disks[d].diskheads[add_h].readingloop, d, p);
            }
            else {
                disks[d].partitions[p].accept_status = 0;
            }
        }


        /////////////////////  推进磁头
        for (int h = 0; h < DISK_HEAD_NUM; h++) {
            int token_left = Num_Tokens;
            int p = disks[d].diskheads[h].current_task_part;
            
            bool can_find_request = false;
            int pt = disks[d].diskheads[h].pointer_location;
            int num_actions = 0;

            if (d == 4 && h == 1 && disks[d].diskheads[h].current_task_part == 1 && disks[d].diskheads[h].cancel_current_part) {
                int a = 1;
            }

            if (!disks[d].part_activated[p]) {
                throw runtime_error("循环中存在被禁用的part");
            }

            if (disks[d].diskheads[h].mission_started && disks[d].units[pt].partition_id == p) {
                int last_u = disks[d].partitions[p].last_unit;
                int need_to_pass = 0;
                for (int u = pt; u <= last_u; u++) {
                    if (disks[d].units[u].number_unfi_req == 0) {
                        need_to_pass++;
                    }
                    else {
                        can_find_request = true;
                        num_actions = need_to_pass;
                        pt = u;
                        token_left -= num_actions;
                        break;
                    }
                }
                if (!can_find_request) {
                    // 如果当前无request的part需要cancel，则禁用改part
                    if (disks[d].diskheads[h].cancel_current_part) {
                        disks[d].part_activated[p] = false;
                        disks[d].partitions[p].accept_status = 0;
                        execute_cancel_part_to_loop(&disks[d].diskheads[h].readingloop, d, p);
                        disks[d].diskheads[h].cancel_current_part = false;
                        // 推进到下一个
                        disks[d].diskheads[h].mission_started = false;
                        disks[d].diskheads[h].current_task_part = disks[d].diskheads[h].readingloop.part_sequence[0];
                        p = disks[d].diskheads[h].current_task_part;
                    }
                    else {
                        //推进循环
                        execute_cycle_part_to_loop(&disks[d].diskheads[h].readingloop);
                        disks[d].diskheads[h].mission_started = false;
                        disks[d].diskheads[h].current_task_part = disks[d].diskheads[h].readingloop.part_sequence[0];
                        p = disks[d].diskheads[h].current_task_part;
                    }

                }
            }
            
            if (!can_find_request) {
                int loop_sz = disks[d].diskheads[h].readingloop.loop_sz;
                for (int i = 0; i < loop_sz; i++) {
                    if (disks[d].partitions[p].num_req < 0) {
                        int a = request[261575].object_id;
                        int b = object[10652].replica_disk[0];
                        int c = object[8427].replica_partition[0];
                        throw runtime_error("任务数量异常");
                    }
                    if (disks[d].partitions[p].num_req != 0) {
                        can_find_request = true;
                        break;
                    }
                    else {
                        // 如果当前无request的part需要cancel，则禁用改part
                        if (disks[d].diskheads[h].cancel_current_part) {
                            disks[d].part_activated[p] = false;
                            disks[d].partitions[p].accept_status = 0;
                            execute_cancel_part_to_loop(&disks[d].diskheads[h].readingloop, d, p);
                            disks[d].diskheads[h].cancel_current_part = false;
                            // 推进到下一个
                            disks[d].diskheads[h].mission_started = false;
                            disks[d].diskheads[h].current_task_part = disks[d].diskheads[h].readingloop.part_sequence[0];
                            p = disks[d].diskheads[h].current_task_part;
                        }
                        else {
                            // 推进循环
                            execute_cycle_part_to_loop(&disks[d].diskheads[h].readingloop);
                            disks[d].diskheads[h].mission_started = false;
                            disks[d].diskheads[h].current_task_part = disks[d].diskheads[h].readingloop.part_sequence[0];
                            p = disks[d].diskheads[h].current_task_part;
                        }
                    }
                }
            }

            if(!can_find_request){
                disks[d].diskheads[h].action_flow = "#\n";
                continue;
            }
            


            // 执行jump或pass 抵达任务区
            if (!disks[d].diskheads[h].mission_started) {
                disks[d].diskheads[h].mission_started = true;
                int start_u = 0;
                int first_u = disks[d].partitions[p].first_unit;
                int last_u = disks[d].partitions[p].last_unit;
                for (int u = first_u; u <= last_u; u++) {
                    if (disks[d].units[u].number_unfi_req > 0) {
                        start_u = u;
                        break;
                    }
                }
                if (start_u == 0){
                    throw runtime_error("没有扫描到任务，跳跃位置异常");
                }

                if (start_u - pt > token_left - 64 || start_u - pt < 0) {
                    // 需要执行jump
                    disks[d].diskheads[h].execute_jump = true;
                    disks[d].diskheads[h].jump_unit = start_u;
                    disks[d].diskheads[h].pointer_location = start_u;
                    continue;
                }
                else {
                    num_actions = start_u - pt;
                    pt = start_u;
                    token_left -= num_actions;
                }

            }

            
            // a* search 推移指针前进;
            a_star_search_diskhead_actionflow(d, h, pt, token_left, num_actions);
            pt = disks[d].diskheads[h].pointer_location;
            int current_task_last_u = disks[d].partitions[p].last_unit;
            if (pt > current_task_last_u) {
                // 当前p完成
                if (disks[d].diskheads[h].cancel_current_part) {
                    // 取消当前p
                    execute_cancel_part_to_loop(&disks[d].diskheads[h].readingloop, d, p);
                    disks[d].diskheads[h].cancel_current_part = false;
                    if (disks[d].diskheads[h].exchange_current_part) {
                        // 置换当前p
                        disks[d].diskheads[h].exchange_current_part = false;
                        int ep = disks[d].diskheads[h].exchange_part;
                        execute_add_part_to_loop(&disks[d].diskheads[h].readingloop, d, ep);
                        disks[d].partitions[ep].can_accept_new_req = true;
                    }
                }
                else {
                    // 不取消当前p, 完成当前p，将当前p推至队尾
                    execute_cycle_part_to_loop(&disks[d].diskheads[h].readingloop);
                }

                // 接取下一p任务
                disks[d].diskheads[h].current_task_part = disks[d].diskheads[h].readingloop.part_sequence[0];
                disks[d].diskheads[h].mission_started = false;
                if (disks[d].units[pt].partition_id == disks[d].diskheads[h].current_task_part) {
                    disks[d].diskheads[h].mission_started = true;
                }
                
                // 考虑是否取消该part
                bool can_cancel = false;
                concider_cancel_part_to_loop(disks[d].diskheads[h].readingloop, &can_cancel);
                if (can_cancel) {
                    int p = disks[d].diskheads[h].current_task_part;
                    disks[d].diskheads[h].cancel_current_part = true;
                    disks[d].partitions[p].accept_status = 1;// 切换为部分接取任务

                    // 如果此时磁针已经在该part扫过一段距离，则修正接取任务的范围
                    if (p == disks[d].units[pt].partition_id) {
                        disks[d].partitions[p].accept_first_u = disks[d].diskheads[h].pointer_location;
                    }
                }
                else {
                    // 考虑是否替代该part
                    bool can_exchange = false;
                    int exchange_part = 0;
                    concider_exchange_part_to_loop(disks[d].diskheads[h].readingloop, d, &can_exchange, &exchange_part, part_iter_sequence);
                    if (can_exchange) {
                        disks[d].diskheads[h].cancel_current_part = true;
                        disks[d].diskheads[h].exchange_current_part = true;
                        disks[d].diskheads[h].exchange_part = exchange_part;
                        disks[d].part_activated[exchange_part] = true;
                        // 如果此时磁针已经在该part扫过一段距离，则修正接取任务的范围
                        if (p == disks[d].units[pt].partition_id) {
                            disks[d].partitions[p].accept_first_u = disks[d].diskheads[h].pointer_location;
                        }
                    }
                }
            }
            else {
                // 没完成当前p的情况下，如果需要取消当前p，更新接受request的范围
                if (disks[d].diskheads[h].cancel_current_part == true) {
                    disks[d].partitions[p].accept_first_u = disks[d].diskheads[h].pointer_location;
                    if (disks[d].partitions[p].accept_first_u > disks[d].partitions[p].accept_last_u) {
                        throw runtime_error("接受request范围异常");
                    }
                }
            }
        }
    }
}




void plan_disk_pointer_init() {
    // 初阶段规划磁针，寻找request所在
    for (int d = 0; d < Num_Disks; d++) {
        for (int h = 0; h < DISK_HEAD_NUM;h++) {
            int c_p = disks[d].diskheads[h].current_task_part;
            if (c_p != 0 && disks[d].partitions[c_p].num_req == 0) {
                disks[d].diskheads[h].currently_free = true;
                disks[d].partitions[c_p].on_reading = false;
            }
            int pt = disks[d].diskheads[h].pointer_location;
            if (disks[d].units[pt].partition_id != c_p) {
                disks[d].diskheads[h].currently_free = true;
                disks[d].partitions[c_p].on_reading = false;
            }
            // 磁针超出管理范围，使其空闲
            if (disks[d].diskheads[h].pointer_location > disks[d].partitions[num_total_tag_part].last_unit) {
                disks[d].diskheads[h].currently_free = true;
            }

            if (disks[d].diskheads[h].currently_free) {
                // 磁针空闲，寻找request
                
                int current_p = disks[d].units[pt].partition_id;
                if (current_p > num_total_tag_part) {
                    current_p = 1;
                }

                bool found_req = false;//用于判断是否找到req;
                for (int q = 0; q < num_total_tag_part; q++) {
                    int p = ((current_p + q - 1) % (num_total_tag_part)) + 1;
                    if (disks[d].partitions[p].num_req > 0 && !disks[d].partitions[p].on_reading) {
                        // 找到request_u
                        found_req = true;
                        disks[d].diskheads[h].current_task_part = p;
                        int req_u = 0;
                        int first_u = disks[d].partitions[p].first_unit;
                        int last_u = disks[d].partitions[p].last_unit;
                        for (int u = first_u; u <= last_u; u++) {
                            if (disks[d].units[u].number_unfi_req > 0) {
                                req_u = u;
                                break;
                            }
                        }
                        // 判断是否需要jump到该位置
                        if (req_u - pt >= 0 && req_u - pt <= Num_Tokens - 64) {
                            //直接read过去;
                            excute_pass_read(d, h, Num_Tokens, 0, 0);
                            disks[d].diskheads[h].currently_free = false;
                            disks[d].diskheads[h].current_partition = p;
                        }
                        else {
                            // jump 到该位置
                            disks[d].diskheads[h].execute_jump = true;
                            disks[d].diskheads[h].jump_unit = req_u;
                            disks[d].diskheads[h].pointer_location = req_u;
                            disks[d].diskheads[h].currently_free = false;
                            disks[d].diskheads[h].current_partition = p;
                        }
                        disks[d].partitions[p].on_reading = true;
                        break;
                    }
                }
                // 如果没找到req，不执行任何动作
                if (!found_req) {
                    disks[d].diskheads[h].action_flow = "#\n";
                }
                continue;
            }
            else {
                // 磁针不空闲 执行pass和read寻找request
                excute_pass_read(d, h, Num_Tokens, 0, 0);
            }
        }
    }
}


void update_pointer_reading_loop() {
    for (int d = 0; d < Num_Disks; d++) {
        for (int h = 0; h < DISK_HEAD_NUM; h++) {
            int loop_sz = disks[d].diskheads[h].readingloop.loop_sz;
            if (loop_sz > 0) {
                for (int i = 0; i < loop_sz; i++) {
                    int p = disks[d].diskheads[h].readingloop.part_sequence[i];
                    disks[d].diskheads[h].readingloop.part_num_105ts_req[i] = disks[d].partitions[p].num_following_105_ts_requests;
                    disks[d].diskheads[h].readingloop.part_ts_ratio[i] = disks[d].partitions[p].estimating_ts_ratio_for_reading;
                }
                disks[d].diskheads[h].readingloop.total_num_105ts_req = sum_vector(disks[d].diskheads[h].readingloop.part_num_105ts_req);
                disks[d].diskheads[h].readingloop.total_ts_ratio = sum_vector(disks[d].diskheads[h].readingloop.part_ts_ratio);
            }
        }
    }
}

/////////////////  对象读取操作（子函数的子函数） 估算 每个disk每个partition在接下来105ts内request总数 和 一个磁针读取part所需要的时间
void estimate_part_num_request_and_time_for_read() {
    for (int d = 0; d < Num_Disks; d++) {
        for (int p = 1; p <= num_total_tag_part; p++) {
            // 估算part中有多少带有request的unit
            disks[d].partitions[p].num_u_with_req = 0;
            disks[d].partitions[p].num_following_105_ts_requests = 0;
            for (int t = 0; t < Num_Tags; t++) {
                // 估算part中有多少request
                disks[d].partitions[p].num_following_105_ts_requests += disks[d].partitions[p].num_tag_units[t] * next_105_tag_req_per_unit[t];
                if (next_105_tag_req_per_unit[t] < 1) {
                    disks[d].partitions[p].num_u_with_req += disks[d].partitions[p].num_tag_units[t] * next_105_tag_req_per_unit[t];
                }
                else {
                    disks[d].partitions[p].num_u_with_req += disks[d].partitions[p].num_tag_units[t];
                }
            }
            // 估算一个磁头读取该part需要的时间
            disks[d].partitions[p].estimating_ts_for_reading = double(disks[d].partitions[p].occupied_u_number) / max_continuous_read + 1;
            double with_req_rate;
            if (disks[d].partitions[p].occupied_u_number == 0) {
                with_req_rate = 0.0;
            }
            else {
                with_req_rate = disks[d].partitions[p].num_u_with_req / disks[d].partitions[p].occupied_u_number;
            }
            if (with_req_rate < unfi_req_unit_rate_for_continuous_read) {
                disks[d].partitions[p].estimating_ts_for_reading *= with_req_rate / unfi_req_unit_rate_for_continuous_read;
            }
            disks[d].partitions[p].estimating_ts_ratio_for_reading = disks[d].partitions[p].estimating_ts_for_reading / total_limit_ts_factor; // 100 是容错 可以更改，不超过105
            // 估算 上报任务量与读取时间的比值
            if (disks[d].partitions[p].estimating_ts_for_reading > 0.0000000001) {
                disks[d].partitions[p].requests_time_ratio = disks[d].partitions[p].num_following_105_ts_requests / disks[d].partitions[p].estimating_ts_for_reading;
            }
            else {
                disks[d].partitions[p].requests_time_ratio = 0;
            }
            
        }
    }
}

//////////////////////  对象读取操作（子函数的子函数） 预测每个tag在接下来105ts内会有多少个request
void predict_next_105_ts_tag_req() {

    int last_50_pt1 = last_50_pt;
    int last_100_pt1 = last_100_pt;
    int last_150_pt1 = current_pt;
    for (int i = 0; i < 50; i++) {
        for (int t = 0; t < Num_Tags; t++) {
            avg_last_0_50_req[t] += last_150_ts_tag_req[t][last_50_pt1];
            avg_last_50_100_req[t] += last_150_ts_tag_req[t][last_100_pt1];
            avg_last_100_150_req[t] += last_150_ts_tag_req[t][last_150_pt1];
        }

        last_50_pt1++;
        last_50_pt1 %= 150;
        last_100_pt1++;
        last_100_pt1 %= 150;
        last_150_pt1++;
        last_150_pt1 %= 150;
    }

    for (int t = 0; t < Num_Tags; t++) {
        avg_last_0_50_req[t] /= 50;
        avg_last_50_100_req[t] /= 50;
        avg_last_100_150_req[t] /= 50;
        current_avg_ts_req_slope[t] += (avg_last_0_50_req[t] - avg_last_50_100_req[t]) / 50;
        current_avg_ts_req_slope[t] += (avg_last_50_100_req[t] - avg_last_100_150_req[t]) / 50;
        current_avg_ts_req_slope[t] /= 2;
        next_105_tag_req[t] = ceil((avg_last_0_50_req[t] + current_avg_ts_req_slope[t] + avg_last_0_50_req[t] + current_avg_ts_req_slope[t] * 105) * 105 / 2);
        if (next_105_tag_req[t] < 0) {
            next_105_tag_req[t] = 0;
        }
        next_105_tag_req_per_unit[t] = double(next_105_tag_req[t]) / num_tag_object[t];//计算得到每个tag在接下来105个ts的tag_u可以获得多少req
    }
}

//////////////////////  对象读取操作（子函数） 存储request
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
        int t = object[object_id].tag;
        currently_tag_unfi_req[t - 1] += sz;
        request[request_id].sz = sz;
        object[object_id].called_times++;
        object[object_id].last_called_ts = ts;
        for (int j = 0; j < sz; j++) {
            request[request_id].have_read[j] = false;
        }
        object[object_id].last_request_point = request_id;

        // 对object 对应的replica的partition和unit增加request_num

        // 记录磁盘每一个unit的读取任务数量
        for (int i = 0; i < REP_NUM; i++) {
            int d = object[object_id].replica_disk[i];
            int p = object[object_id].replica_partition[i];
            disks[d].partitions[p].num_req += sz;
            // 更新disk part req rho
            disks[d].partitions[p].req_rho += double(sz) / disks[d].partitions[p].sz;
            for (int j = 0; j < object[object_id].size; j++) {
                int u = object[object_id].store_units[i][j];
                if (disks[d].units[u].number_unfi_req == 0) {
                    disks[d].partitions[p].req_u_number++;
                }
                disks[d].units[u].number_unfi_req++;
            }
        }

        // 更新 last_150_ts_tag_req
        last_150_ts_tag_req[t - 1][current_pt]++;
        // 更新 request_lose_value_loop
        lose_value_req_loop[time_pointer][loop_pointer[time_pointer]] = request_id;
        loop_pointer[time_pointer]++;

        /////////////// 检查该任务对应的partition状态，若未被激活，不接收任务。

        int d = object[object_id].replica_disk[0];
        int p = object[object_id].replica_partition[0];
        if (disks[d].partitions[p].accept_status == 0) {
            busy_requests[n_busy] = request_id;
            n_busy++;
        }
        else if (!disks[d].partitions[p].accept_status==1) {
            int first_accept_u = disks[d].partitions[p].accept_first_u;
            for (int v = 0; v < sz; v++) {
                int u = object[object_id].store_units[0][v];
                if (u < first_accept_u) {
                    busy_requests[n_busy] = request_id;
                    n_busy++;
                }
            }
        }
    }
    // 更新 current pt 、last 50和last 100 pt
    current_pt++;
    current_pt %= 150;
    last_50_pt++;
    last_50_pt %= 150;
    last_100_pt++;
    last_100_pt %= 150;
}

///////////////////////// 对象读取操作(母函数) read_action
void read_action()
{
    // 存储request
    record_request();
 
    if (ts > 150) {
        //STEP 1: 预测接下来105ts 每个tag 和unit会接收到request的数量
        predict_next_105_ts_tag_req();

        //STEP 2: 估算 每个disk每个partition在接下来105ts内request总数 和 一个磁针读取part所需要的时间
        estimate_part_num_request_and_time_for_read();

        //STEP 3: 更新目前所有磁针loop的总循环时间和价值
        update_pointer_reading_loop();

        //STEP 4: 规划磁盘指针动作(赋予循环)  推进每个disk head 的 reading loop
        plan_disk_pointer_loop();

    }
    else {
        // 初阶段规划磁盘指针 
        plan_disk_pointer_init();

    }

    // 对上报繁忙的任务执行信息更新
    update_busy_request_status();

    // 执行指针动作
    print_pointer_action_and_finished_requsets();
}

//////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////  WRITE  /////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

//////////////////////  对象写入（子函数）  执行对象写入
void execute_object_write(int obj_id, vector<int> w_disk_list, vector<int> w_part_list, vector<vector<int>> w_unit_list, bool over_write, bool continuous_write) {
    //取出object信息
    int sz = object[obj_id].size;
    int tag = object[obj_id].tag;
    if (sz > 3 && continuous_write) {
        if (over_write) {
            over_write = false;
        }
    }

    num_tag_unit[tag - 1] += sz;
    num_tag_object[tag - 1]++;

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
            object[obj_id].store_units[r][v] = u;// 更新object被存储disk上的unit位置
        }

        disks[d].partitions[p].occupied_u_number += sz; //更新partition已被写入的内存
        disks[d].partitions[p].num_tag_units[tag - 1] += sz;
        //disks[d].partitions[p].occupied_tag_u_number[tag - 1] += sz;

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

//////////////////////  对象写入（子函数的子函数）  判断对象能否写入一个partition（无需连续）
void judge_object_write_in_partition(int d, int p, int sz, bool* can_write, vector<int>* w_disk_list, vector<int>* w_part_list, vector<vector<int>>* w_unit_list) {
    (*can_write) = false;
    vector<int> write_unit_list;
    write_unit_list.resize(sz);
    int pt = 0;
    bool reverse_write = disks[d].partitions[p].reverse_write;

    int first_u = disks[d].partitions[p].first_unit;
    int last_u = disks[d].partitions[p].last_unit;

    int num_free_unit = 0; // 记录free的unit数量，用于判断能否完整存储一个object

    for (int u = first_u; u <= last_u; u++) {
        if (disks[d].units[u].occupied) {
            continue;
        }
        else {
            write_unit_list[num_free_unit] = u;
            num_free_unit++;
            if (num_free_unit == sz) {
                *can_write = true;
                break;
            }
        }
    }
    if (*can_write) {
        (*w_disk_list)[0] = d;
        (*w_part_list)[0] = p;
        (*w_unit_list)[0] = write_unit_list;
        for (int j = 0; j < REP_NUM - 1; j++) {
            for (int v = 0; v < sz; v++) {
                int u = write_unit_list[v];
                (*w_disk_list)[j + 1] = disks[d].units[u].rep_disk_id[j];
                (*w_part_list)[j + 1] = disks[d].units[u].rep_part_id[j];
                (*w_unit_list)[j + 1][v] = disks[d].units[u].rep_unit_id[j];
            }
        }
    }
}

//////////////////////  对象写入（子函数的子函数）  判断对象能否连续写入该partition  
void judge_object_continuous_write(int d, int p, bool over_write, int sz, bool* can_write, vector<int>* w_disk_list, vector<int>* w_part_list, vector<vector<int>>* w_unit_list) {
    (*can_write) = false;
    // 对于sz>3的大obj，逆写
    if (sz > 3) {
        if (over_write) {
            over_write = false;
        }
        else {
            over_write = true;
        }
    }

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
        // 给出所有的replica
        (*w_disk_list)[0] = d;
        (*w_part_list)[0] = p;
        for (int v = 0; v < sz; v++) {
            (*w_unit_list)[0][v] = start_u + v;
        }
        for (int j = 0; j < REP_NUM - 1; j++) {
            (*w_disk_list)[j + 1] = disks[d].units[start_u].rep_disk_id[j];
            (*w_part_list)[j + 1] = disks[d].units[start_u].rep_part_id[j];
            for (int v = 0; v < sz; v++) {
                (*w_unit_list)[j + 1][v] = disks[d].units[start_u].rep_unit_id[j] + v;
            }
        }
    }
}

//////////////////////  对象写入（子函数）寻找可写盘的位置
void find_space_for_write(int id, int size, int tag, bool* can_write, vector<int>* w_disk_list, vector<int>* w_part_list, vector<vector<int>>* w_unit_list, bool* over_write, bool* continuous_write) {

    *can_write = false;
    (*w_disk_list).resize(REP_NUM);
    (*w_part_list).resize(REP_NUM);
    (*w_unit_list).resize(REP_NUM);
    for (int i = 0; i < REP_NUM; i++) {
        (*w_unit_list)[i].resize(size);
    }

    // 优先遍历spare unit较多的disk
    vector<int> num_disk_occupied_units;
    num_disk_occupied_units.resize(Num_Disks);
    for (int i = 0; i < Num_Disks; i++) {
        num_disk_occupied_units[i] = SZ_Disks - disks[i].num_spare_units;
    }
    vector<size_t> iter_sequence = sort_indexes(num_disk_occupied_units);

    int t = tag - 1;
    //////////////////////////// 第一轮寻找  常规写入
    for (int i = 0; i < Num_Disks; i++) {
        int d = iter_sequence[i];
        for (int q = 0; q < tag_to_partition[t].partition_list.size(); q++) {
            int p = tag_to_partition[t].partition_list[q];
            if (disks[d].partitions[p].full || disks[d].partitions[p].sz - disks[d].partitions[p].occupied_u_number + 1 < size) {
                continue;
            }
            judge_object_continuous_write(d, p, false, size, can_write, w_disk_list, w_part_list, w_unit_list);
            if (*can_write) {
                *continuous_write = true;
                *over_write = false;
                break;
            }
        }
        if (*can_write) {
            break;
        }
    }

    //////////////////////////// 第二轮寻找  尝试非连续写入相应tag标签的分区
    if (!(*can_write)) {
        for (int i = 0; i < Num_Disks; i++) {
            int d = iter_sequence[i];
            for (int q = 0; q < tag_to_partition[t].partition_list.size(); q++) {
                int p = tag_to_partition[t].partition_list[q];
                if (disks[d].partitions[p].full || disks[d].partitions[p].sz - disks[d].partitions[p].occupied_u_number + 1 < size) {
                    continue;
                }
                judge_object_write_in_partition(d, p, size, can_write, w_disk_list, w_part_list, w_unit_list);
                if (*can_write) {
                    *continuous_write = false;
                    *over_write = false;
                    break;
                }
            }
            if (*can_write) {
                break;
            }
        }
    }

    //////////////////////////// 第三轮寻找  超区写入
    if (!(*can_write)) {
        for (int i = 0; i < Num_Disks; i++) {
            int d = iter_sequence[i];
            for (int q = 0; q < tag_to_partition[t].over_write_partition_list.size(); q++) {
                int p = tag_to_partition[t].over_write_partition_list[q];
                if (disks[d].partitions[p].full || disks[d].partitions[p].sz - disks[d].partitions[p].occupied_u_number + 1 < size) {
                    continue;
                }
                judge_object_continuous_write(d, p, true, size, can_write, w_disk_list, w_part_list, w_unit_list);
                if (*can_write) {
                    *continuous_write = true;
                    *over_write = true;
                    break;
                }
            }
            if (*can_write) {
                break;
            }
        }
    }



    //////////////////////////// 第四轮寻找  尝试非连续超区写入其他tag标签的分区
    if (!(*can_write)) {
        for (int i = 0; i < Num_Disks; i++) {
            int d = iter_sequence[i];
            for (int q = 0; q < tag_to_partition[t].over_write_partition_list.size(); q++) {
                int p = tag_to_partition[t].over_write_partition_list[q];
                if (disks[d].partitions[p].full || disks[d].partitions[p].sz - disks[d].partitions[p].occupied_u_number + 1 < size) {
                    continue;
                }
                judge_object_write_in_partition(d, p, size, can_write, w_disk_list, w_part_list, w_unit_list);
                if (*can_write) {
                    *continuous_write = false;
                    *over_write = true;
                    break;
                }
            }
            if (*can_write) {
                break;
            }
        }
    }

    //////////////////////////// 第五轮寻找  尝试连续写入无tag标签的分区
    // 优先spare_unit较多的disk
    if (!(*can_write)) {
        for (int i = 0; i < Num_Disks; i++) {
            int d = iter_sequence[i];
            for (int q = 0; q < REP_NUM; q++) {
                int p = Num_Tags * REP_NUM + q;
                if (disks[d].partitions[p].full || disks[d].partitions[p].sz - disks[d].partitions[p].occupied_u_number + 1 < size) {
                    continue;
                }
                judge_object_continuous_write(d, p, false, size, can_write, w_disk_list, w_part_list, w_unit_list);
                if (*can_write) {
                    *continuous_write = true;
                    *over_write = false;
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
        for (int i = 0; i < Num_Disks; i++) {
            int d = iter_sequence[i];
            for (size_t p = disks[d].partitions.size() - 1; p >= 1; p--) {
                if (disks[d].partitions[p].full || disks[d].partitions[p].sz - disks[d].partitions[p].occupied_u_number + 1 < size) {
                    continue;
                }
                judge_object_continuous_write(d, p, false, size, can_write, w_disk_list, w_part_list, w_unit_list);
                if (*can_write) {
                    *continuous_write = false;
                    *over_write = false;
                    break;
                }
            }
            if (*can_write) {
                break;
            }
        }
    }

};

////////////////////// 对象写入（母函数） write_action
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
        bool can_write, over_write, continuous_write; // 是否可写入  是否超区写盘  是否连续写入
        vector<int> w_disk_list;
        vector<int> w_part_list;
        vector<vector<int>> w_unit_list;// 盘号 分区号 初始写盘位置

        find_space_for_write(id, size, tag, &can_write, &w_disk_list, &w_part_list, &w_unit_list, &over_write, &continuous_write);

        if (can_write) {
            // 顺利找到写入方案
            execute_object_write(id, w_disk_list, w_part_list, w_unit_list, over_write, continuous_write);
        }
        else {
            // 若未顺利找到写入方案，则需要考虑非连续写入。
            throw runtime_error("未成功写入。");
        }

        //////////   写入完毕, 输出
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

//////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////// DELETE /////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

////////////////////// 对象删除 delete action  
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
        num_tag_unit[tag - 1] -= size;
        num_tag_object[tag - 1]--;
        // 上报需要删除的任务并更新任务状态
        int req_id = object[id].last_request_point;



        while (!request[req_id].is_done) {
            // 更新删除后的request状态
            request[req_id].is_done = true;
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
                //disks[rep_d].partitions[rep_p].occupied_tag_u_number[tag - 1]--;
                //更新partition中未完成的request数量
                int num_unfi_req = disks[rep_d].units[u].number_unfi_req;
                if (num_unfi_req > 0) {
                    disks[rep_d].partitions[rep_p].num_req -= num_unfi_req;
                    disks[rep_d].units[u].number_unfi_req = 0;
                }
                disks[rep_d].partitions[rep_p].num_tag_units[tag - 1]--;
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

//////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////// TimeStamp ///////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

////////////////////// 对齐时间片 timestamp_action   处理urgent_request      
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
        char line[100];
        scanf("%s %d", line, &timestamp);
        printf("TIMESTAMP %d\n", timestamp);
    }


    fflush(stdout);

    // 处理超时request
    time_pointer++;
    if (time_pointer == EXTRA_TIME) {
        time_pointer = 0;
    }
    urgent_loop_pt++;
    urgent_loop_pt %= urgent_request_ts;
    for (int i = 0; i < urgent_req_pt[urgent_loop_pt]; i++) {
        int req_id = urgent_req_loop[urgent_loop_pt][i];
        if (request[req_id].is_done) {
            continue;
        }
        int obj_id = request[req_id].object_id;
        if (object[obj_id].is_urgent) {
            continue;
        }
        object[obj_id].is_urgent = true;
        urgent_obj_list[urgent_obj_list_pt] = obj_id;
        urgent_obj_list_pt++;
        for (int r = 0; r < REP_NUM; r++) {
            int d = object[obj_id].replica_disk[r];
            int p = object[obj_id].replica_partition[r];
            disks[d].partitions[p].is_urgent = true;
            for (int v = 0; v < object[obj_id].size; v++) {
                if (!request[req_id].have_read[v]) {
                    int u = object[obj_id].store_units[r][v];
                    disks[d].units[u].is_urgent = true;
                }
            }
        }
    }

    // 检查是否存在105 ts未完成的任务，若有，抛出错误
    for (int i = 0; i < loop_pointer[time_pointer]; i++) {
        int req_id = lose_value_req_loop[time_pointer][i];
        if (request[req_id].is_done) {
            continue;
        }
        else {
            int c = request[15771215].object_id;
            int a = object[13273].last_request_point;
            int d = object[1902].replica_disk[0];
            int e = object[1902].replica_partition[0];
            int b = disks[2].partitions[2].accept_status;
            int f = disks[2].diskheads[0].readingloop.loop_sz;
            int g = disks[2].diskheads[1].readingloop.loop_sz;
            throw runtime_error("存在任务没有在105ts内上报提交");
        }
    }
    loop_pointer[time_pointer] = 0;


    // 清空此ts的 last_150_ts_tag_req ， 清零 avg_last_0_50_100_150_req
    for (int t = 0; t < Num_Tags; t++) {
        last_150_ts_tag_req[t][current_pt] = 0;
        avg_last_0_50_req[t] = 0.0;
        avg_last_50_100_req[t] = 0.0;
        avg_last_100_150_req[t] = 0.0;
    }
    // 
    
    
}

//////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////// INITIALIZE ///////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////

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

    // 计算tag_on_heat 
    for (int i = 1; i <= Num_Tags; i++) {
        for (int j = 1; j <= (Total_TS - 1) / FRE_PER_SLICING + 1; j++) {
            if (fre_read[i - 1][j - 1] >= tag_on_heat_period_threshold) {
                tag_on_heat[i - 1][j - 1] = true;
            }
            else {
                tag_on_heat[i - 1][j - 1] = false;
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
    int max_part_sz = 20 * max_continuous_read; // 希望一个pt可以在20个ts内读完一个part
    vector<int> tag_part_number;
    tag_part_number.resize(Num_Tags);
    for (int i = 0; i < Num_Tags; i++) {
        tag_part_number[i] = REP_NUM;
        int slice = 1;
        while (true) {
            if (tag_memory_one_part[i] / slice > max_part_sz) {
                slice++;
            }
            else {
                tag_memory_one_part[i] /= slice;
                tag_part_number[i] = slice;
                break;
            }
        }
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
        for (int i = 0; i < Num_Tags; i++) {
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
        int first_p = 1;
        for (int r = 0; r < REP_NUM; r++) {
            for (int i = 0; i < Num_Tags; i++) {
                int t = tag_sequence[i];
                for (int m = 0; m < tag_part_number[t - 1]; m++) {
                    first_p++;
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
    }
    // 归属replica partition 和 unit
    num_total_tag_part = sum_vector(tag_part_number);
    for (int d = 0; d < Num_Disks; d++) {
        int first_p = 1;
        for (int i = 0; i < Num_Tags; i++) {
            for (int m = 0; m < tag_part_number[i]; m++) {
                vector<int> replica_disks;
                vector<int> replica_partitions;
                vector<vector<int>> replica_units;
                replica_disks.resize(REP_NUM);
                replica_partitions.resize(REP_NUM);
                replica_units.resize(REP_NUM);
                for (int r = 0; r < REP_NUM; r++) {
                    replica_disks[r] = (d + r) % 10;
                    replica_partitions[r] = (first_p + r * num_total_tag_part);
                    replica_units[r].resize(disks[d].partitions[first_p + r * num_total_tag_part].sz);
                    for (int u = 0; u < disks[d].partitions[first_p + r * num_total_tag_part].sz; u++) {
                        replica_units[r][u] = disks[d].partitions[first_p + r * num_total_tag_part].first_unit + u;
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
                        for (int u = 0; u < disks[d].partitions[first_p].sz; u++) {
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
                first_p ++;
            }
        }
    }


    // 剩余内存进行无tag分区
    int surplus_sz = SZ_Disks - init_pt[0];
    int no_tag_part_sz = surplus_sz / REP_NUM;
    for (int i = 0; i < Num_Disks; i++) {
        for (int m = 0; m < REP_NUM; m++) {
            Partition part1 = Partition(0, no_tag_part_sz, init_pt[i], true, false);
            disks[i].partitions.push_back(part1);
            init_pt[i] += no_tag_part_sz;
        }
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

    // 归属no tag 的 replica partition 和 unit
    for (int d = 0; d < Num_Disks; d++) {
        vector<int> replica_disks;
        vector<int> replica_partitions;
        vector<vector<int>> replica_units;
        replica_disks.resize(REP_NUM);
        replica_partitions.resize(REP_NUM);
        replica_units.resize(REP_NUM);
        for (int r = 0; r < REP_NUM; r++) {
            replica_disks[r] = (d + r) % 10;
            replica_partitions[r] = (disks[0].partitions.size() - 3 + r);
            replica_units[r].resize(disks[d].partitions[disks[0].partitions.size() - 3 + r].sz);
            for (int u = 0; u < disks[d].partitions[disks[0].partitions.size() - 3 + r].sz; u++) {
                replica_units[r][u] = disks[d].partitions[disks[0].partitions.size() - 3 + r].first_unit + u;
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
                for (int u = 0; u < disks[d].partitions[disks[d].partitions.size() - 3].sz; u++) {
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


    // 归属tag_to_partition
    tag_to_partition.resize(Num_Tags);
    int first_p = 1;
    for (int i = 0; i < Num_Tags / 2; i++) {
        int t1 = tag_sequence[i * 2] - 1;
        int t2 = tag_sequence[i * 2 + 1] - 1;

        for (int m = 0; m < tag_part_number[t1]; m++) {
            tag_to_partition[t1].partition_list.push_back(first_p + m);
        }
        for (int m = 0; m < tag_part_number[t2]; m++) {
            tag_to_partition[t1].over_write_partition_list.push_back(first_p + tag_part_number[t1] + m);
        }
        first_p += tag_part_number[t1];
        first_p += tag_part_number[t2];
        for (int m = 0; m < tag_part_number[t2]; m++) {
            tag_to_partition[t2].partition_list.push_back(first_p - m - 1);
        }
        for (int m = 0; m < tag_part_number[t1]; m++) {
            tag_to_partition[t2].over_write_partition_list.push_back(first_p - tag_part_number[t2] - m - 1);
        }

    }

    //预设partition Evaluator
    for (int d = 0; d < Num_Disks; d++) {
        int p = 1;
        while (p < disks[d].partitions.size()) {
            Partition_Evaluator pe1 = Partition_Evaluator(d, p);
            partition_evaluator.push_back(pe1);
            p += 3;
        }
    }

    // 用来记录前24个part的激活情况
    for (int d = 0; d < Num_Disks; d++) {
        disks[d].part_activated.resize(num_total_tag_part + 1);
        for (int p = 1; p <= num_total_tag_part; p++) {
            disks[d].part_activated[p] = false;
        }
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

    can_read_unit_per_token = double(max_continuous_read) / Num_Tokens;

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

    // 初始化繁忙任务
    busy_requests.resize(50000);

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
        tag_on_heat[i].resize(ceil(double(Total_TS) / 1800));
    }


    // 用于记录urgent_request
    urgent_req_loop.resize(urgent_request_ts);
    urgent_req_pt.resize(urgent_request_ts);
    for (int i = 0; i < urgent_request_ts; i++) {
        urgent_req_loop[i].resize(50000);
        urgent_req_pt[i] = 0;
    }
    urgent_obj_list.resize(10000);

    // 用于记录tag在当前ts是否on heat，是否可以忽略请求
    currently_tag_on_heat.resize(Num_Tags);
    currently_tag_ignore.resize(Num_Tags);
    currently_tag_unfi_req.resize(Num_Tags);
    max_can_process_unit_in_time = Num_Disks * DISK_HEAD_NUM * max_continuous_read * EXTRA_TIME;

    // 用来记录tag在150ts内的请求总量变化值  用于计算tag req 趋势
    last_150_ts_tag_req.resize(Num_Tags);
    for (int i = 0; i < Num_Tags; i++) {
        last_150_ts_tag_req[i].resize(150);
    }
    avg_last_0_50_req.resize(Num_Tags);
    avg_last_50_100_req.resize(Num_Tags);
    avg_last_100_150_req.resize(Num_Tags);
    current_avg_ts_req_slope.resize(Num_Tags);
    next_105_tag_req.resize(Num_Tags);
    next_105_tag_req_per_unit.resize(Num_Tags);
    num_tag_unit.resize(Num_Tags);
    num_tag_object.resize(Num_Tags);
    tag_u_avg_req_next_105_ts.resize(Num_Tags);

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
        scanf("%d%d%d%d%d%d", &Total_TS, &Num_Tags, &Num_Disks, &SZ_Disks, &Num_Tokens, &Num_Exchange);
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

        timestamp_action(); //时间片对齐 + 处理超时的request

        delete_action();//对象删除

        write_action();//对象写入

        int max_called_times_obj;
        int min_called_times_obj;
        int max_called_times = 0;
        int min_called_times = 999999999;
        int called_over_10_obj_num = 0;
        int called_lower_10_obj_num = 0;
        int called_over_100_obj_num = 0;
        int called_lower_100_obj_num = 0;
        int called_over_500_obj_num = 0;
        int called_lower_500_obj_num = 0;
        int called_over_1000_obj_num = 0;
        int called_lower_1000_obj_num = 0;
        int called_over_10000_obj_num = 0;
        int called_lower_10000_obj_num = 0;
        double max_calledtimes_span_ratio = 0.0;
        double min_calledtimes_span_ratio = 9999.0;
        double average_call_times_ratio = 0.0;
        int ratio_lower_than_0_1 = 0;
        int ratio_lower_than_0_01 = 0;
        int ratio_lower_than_0_05 = 0;
        int ratio_lower_than_0_15 = 0;
        int ratio_lower_than_0_005 = 0;
        if (t==40000) {
            // 进行一次object检查，研究有哪那些object接取的任务次数很少
            int i = 1;
            while (object[i].write_in_ts != -1){
                double calledtimes_span_ratio = 0.0;
                if (object[i].is_delete) {
                    calledtimes_span_ratio = double(object[i].called_times) / (object[i].delete_ts - object[i].write_in_ts);
                }
                else {
                    calledtimes_span_ratio = double(object[i].called_times) / (ts - object[i].write_in_ts);
                }
                if (calledtimes_span_ratio > max_calledtimes_span_ratio) {
                    max_calledtimes_span_ratio = calledtimes_span_ratio;
                }
                if (calledtimes_span_ratio < min_calledtimes_span_ratio && calledtimes_span_ratio >0.00000001) {
                    min_calledtimes_span_ratio = calledtimes_span_ratio;
                }
                average_call_times_ratio += calledtimes_span_ratio;
                if (calledtimes_span_ratio < 0.15) {
                    ratio_lower_than_0_15++;
                }
                if (calledtimes_span_ratio < 0.1) {
                    ratio_lower_than_0_1++;
                }
                if (calledtimes_span_ratio < 0.05) {
                    ratio_lower_than_0_05++;
                }
                if (calledtimes_span_ratio < 0.01) {
                    ratio_lower_than_0_01++;
                }
                if (calledtimes_span_ratio < 0.005){
                    ratio_lower_than_0_005++;
                }


                if (object[i].called_times < min_called_times) {
                    min_called_times = object[i].called_times;
                    min_called_times_obj = i;
                }                
                if (object[i].called_times > max_called_times) {
                    max_called_times = object[i].called_times;
                    max_called_times_obj = i;
                }
                if (object[i].called_times <= 10) {
                    called_lower_10_obj_num++;
                }
                else {
                    called_over_10_obj_num++;
                }
                if (object[i].called_times <= 100) {
                    called_lower_100_obj_num++;
                }
                else {
                    called_over_100_obj_num++;
                }
                if (object[i].called_times <= 500) {
                    called_lower_500_obj_num++;
                }
                else {
                    called_over_500_obj_num++;
                }
                if (object[i].called_times <= 1000) {
                    called_lower_1000_obj_num++;
                }
                else {
                    called_over_1000_obj_num++;
                }
                if (object[i].called_times <= 10000) {
                    called_lower_10000_obj_num++;
                }
                else {
                    called_over_10000_obj_num++;
                }
                i++;
            }
            average_call_times_ratio /= i;
        }

        if (disks[7].partitions[5].num_req<0) {
            int a = 1;
        }

        if (disks[7].units[792].number_unfi_req<0) {
            int a = 1;
        }

        read_action();//对象读取

        if (t % 1800 == 0) {
            garbage_collection_action();//回收操作
        }

    }
    //clean();

    return 0;
}
