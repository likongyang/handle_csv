use std::path::PathBuf;
use std::collections::{HashMap, BTreeMap, HashSet};
use std::iter::Iterator;
// use std::fs::File;
// use std::io::{BufRead, BufReader};
// use std::io::prelude::*;
use regex::Regex;
// use serde::{Serialize, Deserialize};

// #[derive(Serialize, Deserialize, Debug)]
// struct Method {
//     combine_company: String,
// }

// #[derive(Serialize, Deserialize, Debug)]
// struct Lixiaoyun {
//     kixiaoyun_source_file: String,
//     kixiaoyun_data_file: String,
//     kixiaoyun_phone_file: String,
// }

// #[derive(Serialize, Deserialize, Debug)]
// struct Tianyancha {
//     tianyancha_source_file: String,
//     tianyancha_phone_file: String,
// }

// #[derive(Serialize, Deserialize, Debug)]
// struct Total {
//     last_time_file: String,
//     combine_tianyancha_file: String,
//     wenlv_file: String,
// }

// #[derive(Serialize, Deserialize, Debug)]
// struct Time {
//     time: String,
// }

// #[derive(Serialize, Deserialize, Debug)]
// struct Conf {
//     method: Option<Method>,
//     lixiaoyun_sourcefile: Option<Lixiaoyun>,
//     tianyancha_sourcefile: Option<Tianyancha>,
//     total_file: Option<Total>,
//     time: Option<Time>,
// }

/// 用于得出单个文件和汇总文件之间的补集
/// 输出汇总文件中不包含单个文件条目中的文件
/// 参数说明：
/// file_path: 单个文件; total_file_path: 汇总文件； target_file_path: 要生成的文件路径；keyword_index：指定要进行对比的关键字所在的列数，从0开始;
/// header：csv文件中是否含有标题行
/// 返回值：输出的文件数据量
fn file_complementary_set(file_path: &PathBuf, total_file_path: &PathBuf, target_file_path: &PathBuf, keyword_index: usize, header: bool) -> u32 {
    // 生产目标输出文件的写指针、单个文件、汇总文件的指针
    let mut wtr = csv::WriterBuilder::new().has_headers(header).from_path(&target_file_path).unwrap();
    let mut rdr_total_file = csv::ReaderBuilder::new().has_headers(header).from_path(&total_file_path).unwrap();

    // 计数器，默认-1则没有任何写入
    let mut count = 0;

    // 如果文件中存在标题行，则处理标题行
    if header {
        wtr.write_record(rdr_total_file.headers().unwrap()).unwrap();
        wtr.flush().unwrap();
    }

    // 补集核心逻辑，总表中指定的列作为hashmap的键，列值作为值，对比文件同理
    // 如果总表的hashmap中含有对比文件的键，则从总表中移除该键
    let mut total_file_hashmap = HashMap::new();
    for total_file_record in rdr_total_file.records() {
        match total_file_record {
            Ok(total_file_record) => {
                let total_file_keyword: String = total_file_record[keyword_index].to_string();
                total_file_hashmap.insert(total_file_keyword, total_file_record);
            },
            Err(_) => eprintln!("could not read total file records"),
        }
    }

    let mut rdr_file = csv::ReaderBuilder::new().has_headers(header).from_path(file_path).unwrap();
    for file_record in rdr_file.records() {
        match file_record {
            Ok(file_record) => {
                let file_keyword: String = file_record[keyword_index].to_string();
                total_file_hashmap.remove(&file_keyword);
            },
            Err(_) => eprintln!("could not read file records"),
        }
    }

    // 剩下的键写入数据
    for key in total_file_hashmap.keys() {
        let file_record = total_file_hashmap.get(key);
        match file_record {
            Some(file_record) => {
                wtr.write_record(file_record).unwrap();
                count += 1;
            },
            _ => eprintln!("could not get file record"),
        }
    }
    wtr.flush().unwrap();
    count
}

/// 用于多个文件之间并集，同一来源之间
/// 输出并集文件
/// 参数说明：
/// file_path:多个文件路径组成的vector, target_file_path：最终生成文件的路径；keyword_index：指定要进行对比的关键字所在的列数，从0开始;
/// header：csv文件中是否含有标题行
/// 返回值：输出的文件数据量，新增的数据量
fn multiple_file_union(file_path: Vec<PathBuf>, target_file_path: &PathBuf, keyword_index: usize, header: bool) -> (u32, u32) {
    // 计数器，默认0则没有任何写入
    let mut count = 0;
    let mut new = 0;

    // 生产目标文件的指针，以及多个文件中第一个文件的读取指针
    let mut wtr = csv::WriterBuilder::new().has_headers(header).from_path(target_file_path).unwrap();
    let mut rdr_first_file = csv::ReaderBuilder::new().has_headers(header).from_path(&file_path[0]).unwrap();
    
    // 如果包含行标题，则写入行标题
    if header {
        wtr.write_record(rdr_first_file.headers().unwrap()).unwrap();
        wtr.flush().unwrap();
    }

    // 核心算法：
    // 以对每个文件的某一个列进行hash处理，指定列值为键，行作为值
    // 利用hashmap更新的机制，把所有的文件的列和值都生成hashmap后再进行写入
    let mut file_hashmap = HashMap::new();
    for file_index in 0..file_path.len() {
        let mut rdr_file = csv::ReaderBuilder::new().has_headers(header).from_path(&file_path[file_index]).unwrap();
        for file_record in rdr_file.records() {
            match file_record {
                Ok(file_record) => {
                    let file_keyword: String = file_record[keyword_index].to_owned();
                    if file_hashmap.contains_key(&file_keyword) {
                        continue;
                    } else {
                        file_hashmap.insert(file_keyword, file_record);
                        if file_index == 1 {
                            new += 1;
                        }
                    }
                },
                Err(_) => eprintln!("could not read file records"),
            }
        }
    }

    // 写入到文件中
    for key in file_hashmap.keys() {
        let file_record = file_hashmap.get(key);
        match file_record {
            Some(file_record) => {
                wtr.write_record(file_record).unwrap();
                count += 1;
            },
            _ => eprintln!("could not read file record"),
        }
    } 

    wtr.flush().unwrap();

    (count, new)
}

/// 用于多个文件之间交集
/// 输出交集文件
/// 参数说明：
/// file_path:多个文件路径组成的vector, target_file_path：最终生成文件的路径；keyword_index：指定要进行对比的关键字所在的列数，从0开始;
/// header：csv文件中是否含有标题行
/// 返回值：输出的文件数据量
fn multiple_file_intersection(file_path: Vec<PathBuf>, target_file_path: &PathBuf, keyword_index: usize, header: bool) -> u32 {
    // 计数器，默认0则没有任何写入
    let mut count = 0;

    // 生产目标文件的指针，以及多个文件中第一个文件的读取指针
    let mut wtr = csv::WriterBuilder::new().has_headers(header).from_path(target_file_path).unwrap();
    let mut rdr_first_file = csv::ReaderBuilder::new().has_headers(header).from_path(&file_path[0]).unwrap();
    
    // 如果包含行标题，则写入行标题
    if header {
        wtr.write_record(rdr_first_file.headers().unwrap()).unwrap();
        wtr.flush().unwrap();
    }

    // 核心算法：
    // 以第一个文件作为基础文件，递归求文件之间的交集，直至到最后一个文件
    let mut compare_hashmap = HashMap::new();
    for first_file_record in rdr_first_file.records() {
        match first_file_record {
            Ok(first_file_record) => {
                let file_keyword: String = first_file_record[keyword_index].to_string();
                compare_hashmap.insert(file_keyword, first_file_record);
            },
            Err(_) => eprintln!("could not read file record"),
        }
    }


    for file_index in 1..file_path.len() {
        let mut temp_hashmap = HashMap::new();
        let mut rdr_file = csv::ReaderBuilder::new().has_headers(header).from_path(&file_path[file_index]).unwrap();

        for file_record in rdr_file.records() {
            match file_record {
                Ok(file_record) => {
                    let file_keyword: String = file_record[keyword_index].to_string();
                    if compare_hashmap.contains_key(&file_keyword) {
                        temp_hashmap.insert(file_keyword, file_record);
                    }
                },
                Err(_) => eprintln!("could not read file record"),
            }
        }
        
        // 转移临时hashmap作为对比的hashmap
        // 判断如果没有了交集，则退出；如果到了最后一个文件，则把比较的结果输出文件
        compare_hashmap = temp_hashmap.to_owned();
        if compare_hashmap.is_empty() {
            count = 0;
            break;
        } else if (file_index + 1) == file_path.len() {
            for key in compare_hashmap.keys() {
                let file_record = compare_hashmap.get(key);
                match file_record {
                    Some(file_record) => {
                        wtr.write_record(file_record).unwrap();
                        count += 1;
                    },
                    _ => eprintln!("could get file record"),
                }
            }
            break;
        }
        temp_hashmap.clear();
    }

    count
}

/// 用于对指定关键字进行合并，其中号码部分进行合并
/// 输出合并完毕后的文件（公司名称一项不会有重复结果）
/// 参数说明：
/// file_path：要进行合并的数据源文件路径，target_file_path：最终生成文件的路径，header：csv文件是否含有标题行
/// keyword_index：指定合并数据的依据列，从0开始
fn same_format_combine_data(file_path: &PathBuf, target_file_path: &PathBuf, header:bool, keyword_index: usize) {
    // 生产目标文件的指针，以及多个文件中第一个文件的读取指针
    let mut wtr = csv::WriterBuilder::new().has_headers(header).from_path(target_file_path).unwrap();
    let mut rdr_file = csv::ReaderBuilder::new().has_headers(header).from_path(file_path).unwrap();
    
    // 如果包含行标题，则写入行标题
    if header {
        wtr.write_record(rdr_file.headers().unwrap()).unwrap();
        wtr.flush().unwrap();
    }

    // 逻辑:
    // 轮询第一个字段，如果字段名字相同，则合并为一个公司，同时比较第四列手机号码和第五列固定电话
    // 比较号码前先判断是否为空，如果为空则跳过，如果不为空，暂时保留组装到vector中
    // 如果判断第一个字段不相等，则暂存，然后组装上一轮获得的结果，并写入
    // TODO: 判断文件到了最后的写出，目前还不能
    let mut temp_string = String::from("");
    let none_value = String::from("");
    let mut mobile_phone: String = String::from("");
    let mut tel_phone: String = String::from("");
    let mut record_for_write: Vec<String> = vec![];
    let mut first_flag = true;
    for record in rdr_file.records() {
        match record {
            Ok(record) => {
                let compare_data = record[keyword_index].to_string();
                if first_flag {
                    for i in 0..record.len() {
                        record_for_write.push(record[i].to_string());
                        temp_string = compare_data.to_string();
                    }

                    if record[3] != none_value {
                        mobile_phone.push_str(&record[3]);
                        mobile_phone.push_str(";");
                    }
                    if record[4] != none_value {
                        tel_phone.push_str(&record[4]);
                        tel_phone.push_str(";");
                    }
                    first_flag = false;
                } else {
                    if compare_data == temp_string {
                        if record[3] != none_value {
                            mobile_phone.push_str(&record[3]);
                            mobile_phone.push_str(";");
                        }
                        if record[4] != none_value {
                            tel_phone.push_str(&record[4]);
                            tel_phone.push_str(";");
                        }
                    } else {
                        record_for_write[3] = mobile_phone.to_string();
                        record_for_write[4] = tel_phone.to_string();
                        wtr.write_record(&record_for_write).unwrap();
                        wtr.flush().unwrap();
                        record_for_write.clear();
                        mobile_phone.clear();
                        tel_phone.clear();

                        for i in 0..record.len() {
                            record_for_write.push(record[i].to_string());
                            temp_string = compare_data.to_string();
                        }

                        if record[3] != none_value {
                            mobile_phone.push_str(&record[3]);
                            mobile_phone.push_str(";");
                        }
                        if record[4] != none_value {
                            tel_phone.push_str(&record[4]);
                            tel_phone.push_str(";");
                        }
                    }
                }
                
            },
            Err(_) => eprintln!("Could not read file")
        }
    }
}

/// 合并多个表（并集），不同来源
/// 号码判断，如果号码则完全合并，否则写入第二列联系方式，同时改写数据源，输出合并后的文件及合并条目数量
/// 参数说明:
/// file_path_vec要合并的文件组路径；target_file_path：生成的目标文件路径；header：csv文件否是含标题行
/// keyword_index：指定的合并依据关键字
/// 返回值：总的输出文件数据量，非独立来源的数据量，独立来源的数据量
fn combine_data_from_differen_source(file_path_vec: Vec<PathBuf>, target_file_path: &PathBuf, header:bool, keyword_index: usize) -> (u32, u16, u16) {
    // 计数器
    let mut count = 0;
    let mut new = 0;
    let mut union = 0;
    // file_path: Vec<PathBuf>,  keyword_index: usize, header: bool
    // 生产目标文件的指针，以及多个文件中第一个文件的读取指针
    let mut wtr = csv::WriterBuilder::new().has_headers(header).from_path(target_file_path).unwrap();
    let mut rdr_first_file = csv::ReaderBuilder::new().has_headers(header).from_path(&file_path_vec[0]).unwrap();
    
    // 如果包含行标题，则写入行标题
    if header {
        wtr.write_record(rdr_first_file.headers().unwrap()).unwrap();
        wtr.flush().unwrap();
    }

    // 逻辑：
    // 以对每个文件的某一个列进行hash处理，指定列值为键，行作为值
    // 利用hashmap的键为唯一性，对接下来的文件指定关键字进行对比
    // 如果键相同，则获取当前hashmap中的键值，并且修改第二列(15)联系方式，同时把来源更改为指定来源，写入
    // 否则hashmap插入新值
    // 最后写入hashmap中的键值
    let mut file_hashmap = HashMap::new();
    let mut record_for_write: Vec<String> = vec![];
    // let mut phone_second: String = String::from("");
    let source: String = String::from("1号来源；2号来源");

    for file_index in 0..file_path_vec.len() {
        let mut rdr_file = csv::ReaderBuilder::new().has_headers(header).from_path(&file_path_vec[file_index]).unwrap();
        for file_record in rdr_file.records() {
            match file_record {
                Ok(file_record) => {
                    let file_keyword: String = file_record[keyword_index].to_string();
                    if file_hashmap.contains_key(&file_keyword) {
                        let file_record_first: std::option::Option<&csv::StringRecord> = file_hashmap.get(&file_keyword);
                        match file_record_first {
                            Some(file_record_first) => {
                                let phone_two_string = &file_record[15];
                                for i in 0..file_record_first.len() {
                                    record_for_write.push(file_record_first[i].to_string());
                                }
                                record_for_write[0] = source.to_string();
                                record_for_write[15] = phone_two_string.to_string();
                                wtr.write_record(&record_for_write).unwrap();
                                wtr.flush().unwrap();
                                file_hashmap.remove(&file_keyword);
                                record_for_write.clear();
                                count += 1;
                                if file_index == 1 {
                                    new += 1;
                                }
                            },
                            _ => eprintln!("could not read file record"),
                        }
                    } else {
                        file_hashmap.insert(file_keyword, file_record);
                        if file_index == 1 {
                            new += 1;
                            union += 1;
                        }
                    }
                },
                Err(_) => eprintln!("could not read file records"),
            }
        }
    }

    // 写入到文件中
    for key in file_hashmap.keys() {
        let file_record = file_hashmap.get(key);
        match file_record {
            Some(file_record) => {
                wtr.write_record(file_record).unwrap();
                count += 1;
            },
            _ => eprintln!("could not read file record"),
        }
    } 

    wtr.flush().unwrap();

    (count, new, union)
}

/// 合并电话
/// 输出合并电话后的文件
/// 参数说明:
/// file_path 指定的源数据文件；target_file_path:最终生成的文件路径：header:csv文件中是否含有标题行
/// source：数据来源，如果是1则第二列并入第一列，如果是2，则第一列并入第二列，清空不并入的那一列
fn combine_phone(file_path: &PathBuf, target_file_path: &PathBuf, source: u16, header: bool) {
    // 生产目标输出文件的写指针、单个文件的读取指针
    let mut wtr = csv::WriterBuilder::new().has_headers(header).from_path(&target_file_path).unwrap();
    let mut rdr_file = csv::ReaderBuilder::new().has_headers(header).from_path(file_path).unwrap();

    // 如果文件中存在标题行，则处理标题行
    if header {
        wtr.write_record(rdr_file.headers().unwrap()).unwrap();
        wtr.flush().unwrap();
    }

    let mut record_for_write: Vec<String> = vec![];
    let mut phone_hashset = HashSet::new();     // 利用hashset进行去重
    let mut phone_second: String = String::from("");
    let mut column = 14;
    let mut un_column = 15;
    let mut data_from = "1号来源";

    if source == 2 {
        column = 15;
        un_column = 14;
        data_from = "2号来源";
    }

    // 逻辑：
    // 对第一列联系方式按分号进行分割，并放进hashset里
    // 对第二列联系方式按分号进行分割，并放进hashset里
    // 从hashset中组装联系方式，放进source的column列，同时清空un_column列数据
    // 写入文件，清空临时容器
    for file_record in rdr_file.records() {
        match file_record {
            Ok(file_record) => {
                for i in 0..file_record.len() {
                    record_for_write.push(file_record[i].to_string());
                }

                let phone_one_string = &file_record[column];
                let phone_two_string = &file_record[un_column];
                let phone_one_vector:Vec<&str> = phone_one_string.split(";").collect();
                let phone_two_vector:Vec<&str> = phone_two_string.split(";").collect();
                for phone in phone_one_vector {
                    phone_hashset.insert(phone.to_string());
                }
                for phone in phone_two_vector {
                    phone_hashset.insert(phone.to_string());
                }
                for phone in &phone_hashset {
                    phone_second.push_str(&phone);
                    phone_second.push_str(";");
                }
                // }

                record_for_write[0] = data_from.to_string();
                let phone = mobile_first(phone_second.to_owned());
                record_for_write[column] = phone;
                record_for_write[un_column] = String::from("");

                wtr.write_record(&record_for_write).unwrap();
                wtr.flush().unwrap();
                phone_second.clear();
                phone_hashset.clear();
                record_for_write.clear();
            },
            Err(_) => eprintln!("could not read total file records"),
        }
    }
}

/// 指定关键字在指定的文件中进行搜索
/// 输出搜索结果文件
/// 参数说明：
/// keyword: 支持单个或多个关键字组成的vector，file_path：指定的源数据文件；target_file_path:最终生成的文件路径
/// header:csv文件中是否含有标题行；search_index: 要进行搜索时列数，从0开始
/// 返回值：对应关键字的数据量
fn search_keyword(keyword: Vec<&str>, file_path: &PathBuf, target_file_path: &PathBuf, header: bool, search_index: usize) -> u32 {
    // 计数器
    let mut count = 0;

    // 生产文件读取和写入指针
    let mut rdr_file = csv::ReaderBuilder::new().has_headers(header).from_path(file_path).unwrap();
    let mut wtr = csv::WriterBuilder::new().has_headers(header).from_path(target_file_path).unwrap();

    if header {
        wtr.write_record(rdr_file.headers().unwrap()).unwrap();
        wtr.flush().unwrap();
    }

    for record in rdr_file.records() {
        match record {
            Ok(record) => {
                let query: &str = &record[search_index];
                for i in 0..keyword.len() {
                    if query.contains(&keyword[i]) {
                        wtr.write_record(&record).unwrap();
                        count += 1;
                        wtr.flush().unwrap();
                        break;
                    }
                }
            },
            Err(_) => eprintln!("could not read file"),
        }
    }

    count
}

/// 指定关键字在指定的文件多列中进行搜索
/// 输出搜索结果文件
/// 参数说明：
/// keyword: 支持单个或多个关键字组成的vector，file_path：指定的源数据文件；target_file_path:最终生成的文件路径
/// header:csv文件中是否含有标题行；search_index: 要进行搜索时列数vector，列数从0开始
/// 返回值：对应关键字的数据量
fn multiple_search_keyword(keyword: Vec<&str>, file_path: &PathBuf, target_file_path: &PathBuf, header: bool, search_index: Vec<usize>) -> u32 {
    // 计数器
    let mut count = 0;

    // 生产文件读取和写入指针
    let mut rdr_file = csv::ReaderBuilder::new().has_headers(header).from_path(file_path).unwrap();
    let mut wtr = csv::WriterBuilder::new().has_headers(header).from_path(target_file_path).unwrap();

    if header {
        wtr.write_record(rdr_file.headers().unwrap()).unwrap();
        wtr.flush().unwrap();
    }

    for record in rdr_file.records() {
        match record {
            Ok(record) => {
                for si in search_index.iter() {
                    let mut search_result = false;
                    // println!("i为{}, search_result为{}", si, search_result);
                    let query: &str = &record[*si];
                    // println!("query为{}", query);
                    for j in 0..keyword.len() {
                        if query.contains(&keyword[j]) {
                            wtr.write_record(&record).unwrap();
                            count += 1;
                            wtr.flush().unwrap();
                            search_result = true;
                            break;
                        }
                    }
                    if search_result {
                        break;
                    }
                }
            },
            Err(_) => eprintln!("could not read file"),
        }
    }

    count
}

/// 指定关键字从指定的文件中进行剔除
/// 输出剔除相关数据后剩余数据的文件
/// 参数说明
/// keyword: 支持单个或多个关键字组成的vector，file_path：指定的源数据文件；target_file_path:最终生成的文件路径
/// header:csv文件中是否含有标题行；search_index: 要进行搜索时列数，从0开始
/// 返回值：对应关键字的数据量
fn delete_keyword_data(keyword: Vec<&str>, file_path: &PathBuf, target_file_path: &PathBuf, header: bool, search_index: usize) -> u32 {
    // 计数器
    let mut count = 0;

    // 生产文件读取和写入指针
    let mut rdr_file = csv::ReaderBuilder::new().has_headers(header).from_path(file_path).unwrap();
    let mut wtr = csv::WriterBuilder::new().has_headers(header).from_path(target_file_path).unwrap();
    
    if header {
        wtr.write_record(rdr_file.headers().unwrap()).unwrap();
        wtr.flush().unwrap();
    }

    let mut flag = true;
    for record in rdr_file.records() {
        match record {
            Ok(record) => {
                let query: &str = &record[search_index];
                for i in 0..keyword.len() {
                    if query.contains(keyword[i]) {
                        flag = false;
                        break;
                    }
                }
                if flag {
                    wtr.write_record(&record).unwrap();
                    count += 1;
                    wtr.flush().unwrap();
                }
                flag = true;
            },
            Err(_) => eprintln!("could not read file"),
        }
    }

    count
}

/// 去重式搜索输出
/// 输出对应各自关键字的文件，每次搜索基于总数据（迭代）剔除上一次搜索输出的数据进行搜索
/// 参数说明:
/// keyword_map：由不同关键字组成的map,key对应文旅的输出类型; file_path：指定的源数据文件, tfp_map：最终生成的文件路径组成的map，key对应文旅的输出类型
/// header: csv文件中是否含有标题行; search_index：要进行搜索时的列数，从0开始，
/// 返回值：对应每一组关键字的数据量
fn unique_search_keyword(keyword_map: BTreeMap<&str, Vec<&str>>, file_path: &PathBuf, tfp_map: BTreeMap<&str, PathBuf>, header: bool, search_index: usize) -> HashMap<String, u32> {
    // 计数器，分别对每个关键字进行统计生成的数据量
    let mut keyword_result_count:HashMap<String, u32> = HashMap::new();

    // 生成源文件读取指针，读取源文件到hashmap中，以方便后续进行搜索和剔除
    let mut rdr_file = csv::ReaderBuilder::new().has_headers(header).from_path(&file_path).unwrap();
    let mut file_record_map = HashMap::new();
    for record in rdr_file.records() {
        match record {
            Ok(record) => {
                let key = record[1].to_owned();
                file_record_map.insert(key, record.to_owned());
            },
            Err(_) => eprintln!("could not read file"),
        }
    }

    // 对每组关键字进行搜索并输出文件,以关键字的归类为主索引，分组搜索、生成目标文件
    for &key in keyword_map.keys() {
        let mut count = 0;
        let mut for_remove = vec![];
        let target_file_path = tfp_map.get(key);        // 目标文件地址
        match target_file_path {
            Some(target_file_path) => {
                let mut wtr = csv::WriterBuilder::new().has_headers(header).from_path(target_file_path).unwrap();

                // 标题行生成
                if header {
                    wtr.write_record(rdr_file.headers().unwrap()).unwrap();
                    wtr.flush().unwrap();
                }

                // 对每一条数据，进行该组关键字搜索，如果存在则写入到目标文件中，同时存储该条数据到一个临时的容器
                for (record_key, record_value) in &file_record_map {
                    let query: &str = &record_value[search_index];
                    let keyword_vector = keyword_map.get(key);
                    match keyword_vector {
                        Some(keyword_vector) => {
                            for &kv in keyword_vector {
                                if query.contains(&kv) {
                                    wtr.write_record(record_value).unwrap();
                                    wtr.flush().unwrap();
                                    count += 1;
                                    for_remove.push(record_key.to_owned());
                                    break;
                                }
                            }
                        },
                        _ => eprintln!("somethng wrong with this key {} when read keyword map", key),
                    }
                }

                // 从hashmap中删除该组文件中已经输出的数据
                for record_key in for_remove.iter() {
                    file_record_map.remove(record_key);
                }
                for_remove.clear();
                
                keyword_result_count.insert(key.to_owned(), count);     // 统计输出数量
            },
            _ => eprintln!("something wrong with this key {}", key),
        }
    }

    keyword_result_count

}

/// 手机号码优先
/// 返回经过重排后的数据，其中手机号码在前，固定电话在后
/// 参数说明：
/// phone：联系方式的字符串
/// 返回值：手机在前的联系方式字符串
fn mobile_first(phone: String) -> String {
    let change_to_str = phone.as_str();
    let re = Regex::new(r"1\d{10};").unwrap();

    let mut mobile_vec = Vec::new();

    for result in re.captures_iter(change_to_str) {
        mobile_vec.push(result[0].to_owned())
    }

    let telephone = re.replace_all(change_to_str, "");
    let mut result = String::from("");

    for mv in mobile_vec {
        result.push_str(&mv);
    }

    result.push_str(&telephone);

    result
}

fn main() {
    // 测试文件路径
    // let file_one_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/20201213/文旅公司信息列表-规划类-公司-20201213.csv");
    // let file_one_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/20201208/文旅信息列表-天眼查-20201208.csv");
    // let file_two_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/20201213/文旅公司信息列表-规划类-公司2-20201213.csv");
    // let file_three_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/20201213/文旅公司信息列表-规划类-经营-20201213.csv");
    // let file_four_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/lixiaoyun/combine_lixiaoyun_20201204.csv");
    // let file_two_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/lixiaoyun/phone_lixiaoyun_0102.csv");
    let total_file_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/test_program/文旅公司信息列表-未打-20201213.csv");
    // let target_file_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/lixiaoyun/文旅外呼列表_硬件集成商_20201204.csv");
    let target_file_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/test_program/test-20201214.csv");
    // let file_path_vec = vec![file_one_path, file_two_path, file_three_path];
    // let file_path_vec = vec![file_one_path, file_three_path];
    // let file_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/20201201/lixiaoyun_20201201.csv");
    // 函数开始运行时间
    let start_time = std::time::Instant::now();

    // let mut rdr_file = csv::ReaderBuilder::new().has_headers(true).from_path(&file_one_path).unwrap();
    // let mut keyword = Vec::new();
    // let mut style = String::from("");

    // for file_record in rdr_file.records() {
    //     match file_record {
    //         Ok(file_record) => {
    //             let temp_str = file_record[1].trim().replace("有限公司","");
    //             if temp_str != "" {
    //                 // keyword.push(temp_str);
    //                 style.push_str(temp_str.as_str());
    //             }
    //         },
    //         Err(_) => eprintln!("can not read data"),
    //     }
    // }

    // println!("{:?}", style);


    // let file_path = "Source.toml";
    // let mut file = match File::open(file_path) {
    //     Ok(f) => f,
    //     Err(e) => panic!("no such file {} exception: {}", file_path, e)
    // };

    // let mut str_val = String::new();
    // match file.read_to_string(&mut str_val) {
    //     Ok(s) => s,
    //     Err(e) => panic!("Error reading file: {}", e)
    // };

    // let config: Conf = toml::from_str(&str_val).unwrap();

    // 合并电话一项
    // combine_phone(&file_one_path, &target_file_path, 2, true);
    // 相同公司多条线索合并
    // same_format_combine_data(&file_one_path, &target_file_path, true, 1);

    // 补集
    // let count = file_complementary_set(&file_one_path, &total_file_path, &target_file_path, 1, true);

    // 多个文件之间的并集
    // let (count, new) = multiple_file_union(file_path_vec, &target_file_path, 1, true);

    // 不同来源文件并集
    // let (count, new, union) = combine_data_from_differen_source(file_path_vec, &target_file_path, true, 1);

    // 交集
    // let count = multiple_file_intersection(file_path_vec, &target_file_path, 1, true);

    // 搜索指定的关键字数据
    // let guihua_keyword = vec!["旅游设计规划", "景区设计规划", "旅游景区规划", "展览展示策划", "景观设计", "文旅规划咨询", "景区策划", 
                                // "园林景观设计", "旅游文化活动组织策划"];
    // let hongse_keyword = vec!["红色文旅", "红色教育"];
    // let guanli_keyword = vec!["景区游览", "游览景区管理", "景区管理", "旅游节庆活动", "景区运营"];
    // let ruanjian_keyword = vec!["技术开发", "技术服务", "软件开发", "计算机技术服务"];
    // let yingjian_keyword = vec!["建筑工程", "基础设施建设"];
    // let kaifa_keyword = vec!["旅游景点开发", "旅游景区开发", "旅游景点投资", "景区景点开发", "旅游景区管理", "景区建设及经营", "旅游资源开发建设", 
                                // "观光旅游", "旅游资源开发", "旅游项目开发", "旅游景点开发与经营"];


    let keyword = vec!["旅游设计规划", "景区设计规划", "旅游景区规划", "展览展示策划", "景观设计", "文旅规划咨询", "景区策划", 
                                "园林景观设计", "旅游文化活动组织策划"];
    // let keyword = vec!["旅游文化", "文化旅游"];
    // let keyword = vec!["红色文旅", "红色教育"];
    // let guanli_keyword = vec!["景区游览", "游览景区管理", "景区管理", "旅游节庆活动", "景区运营"];
    // let ruanjian_keyword = vec!["技术开发", "技术服务", "软件开发", "计算机技术服务"];
    // let yingjian_keyword = vec!["建筑工程", "基础设施建设"];
    // let kaifa_keyword = vec!["旅游景点开发", "旅游景区开发", "旅游景点投资", "景区景点开发", "旅游景区管理", "景区建设及经营", "旅游资源开发建设", 
                                // "观光旅游", "旅游资源开发", "旅游项目开发", "旅游景点开发与经营"];
    // let count = search_keyword(keyword, &total_file_path, &target_file_path, true, 1);
    let search_index = vec![1, 21];
    let count = multiple_search_keyword(keyword, &total_file_path, &target_file_path, true, search_index);

    // 剔除指定的关键字数据
    // let keyword = vec!["旅行社", "船舶", "法律", "汽车客运", "旅客运输", "贸易有限公司", "商贸有限公司", "生物科技", "国际旅游", "农业发展", "餐饮管理",
    //                             "金属材料", "汽车运输", "旅游集散中心", "职业技术学院", "汽车服务", "客运有限公司", "杂志社", "航空客户运输", "户外协会"];
    // let keyword = vec!["汽车租赁", "旅游车队","服饰","农场","摄影家协会","农林投资","研究会","房车","游艇","影视制作","游客服务中心","五金",
    //                             "商务服务", "中心", "纪念品", "公益", "航空", "学院", "生态", "农业"];
    // let keyword = vec!["蔬菜", "汽车租赁", "鞋帽"];
    // let keyword = vec!["旅行社", "船舶", "法律", "汽车客运", "旅客运输"];
    // let keyword =  vec!["贸易有限公司", "商贸有限公司", "生物科技", "国际旅游", "农业发展", "餐饮管理","金属材料", "汽车运输", "旅游集散中心",
    //                             "职业技术学院", "汽车服务", "客运有限公司", "杂志社", "航空客户运输", "户外协会","汽车租赁", "旅游车队","服饰","农场",
    //                             "摄影家协会","农林投资","研究会","房车","游艇","影视制作","游客服务中心","五金","商务服务", "中心", "纪念品", "公益", 
    //                             "航空", "学院", "生态", "农业"];
    // let count = delete_keyword_data(keyword, &total_file_path, &target_file_path, true, 1);
    
    // 去重式搜索生成文件
    // let mut keyword_map = BTreeMap::new();
    // let mut tfp_map = BTreeMap::new();
    // keyword_map.insert("1guihua", guihua_keyword);
    // keyword_map.insert("2hongse", hongse_keyword);
    // keyword_map.insert("3guanli", guanli_keyword);
    // keyword_map.insert("4ruanjian",ruanjian_keyword);
    // keyword_map.insert("5yingjian", yingjian_keyword);
    // keyword_map.insert("6kaifa", kaifa_keyword);

    // let file_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/test_program/lixiaoyun.csv");
    // let guihua_file_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/test_program/guihua.csv");
    // let hongse_file_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/test_program/hongse.csv");
    // let guanli_file_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/test_program/guanli.csv");
    // let ruanjian_file_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/test_program/ruanjian.csv");
    // let yingjian_file_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/test_program/yingjian.csv");
    // let kaifa_file_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/test_program/kaifa.csv");
    // tfp_map.insert("1guihua", guihua_file_path);
    // tfp_map.insert("2hongse", hongse_file_path);
    // tfp_map.insert("3guanli", guanli_file_path);
    // tfp_map.insert("4ruanjian", ruanjian_file_path);
    // tfp_map.insert("5yingjian", yingjian_file_path);
    // tfp_map.insert("6kaifa", kaifa_file_path);

    // let result = unique_search_keyword(keyword_map, &file_path, tfp_map, true, 21);
    // let mut total = 0;
    // for (_, v) in &result {
    //     total += v;
    // }


    let end_time = std::time::Instant::now();
    let cost_time = end_time.duration_since(start_time);
    println!("耗时{:?}", cost_time);
    // println!("{:#?}", result);
    println!("共生成 {} 条数据", count);


    // println!("一共有 {} 条数据生成， 新增 {} 条数据, 独立来源 {}", count, new, union);
    // println!("一共有 {} 条数据生成， 新增 {} 条数据", count, new);
}