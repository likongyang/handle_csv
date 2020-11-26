use std::path::PathBuf;
use std::collections::HashMap;
use std::iter::Iterator;
use clap::{App, Arg};

/// 用于得出单个文件和汇总文件之间的补集
/// 输出汇总文件中不包含单个文件条目中的文件
/// 参数说明：
/// file_path: 单个文件; total_file_path: 汇总文件； target_file_path: 要生成的文件路径；keyword_index：指定要进行对比的关键字所在的列数，从0开始;
/// header：csv文件中是否含有标题行
fn file_complementary_set(file_path: &PathBuf, total_file_path: &PathBuf, target_file_path: &PathBuf, keyword_index: usize, header: bool) -> i32 {
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
                let total_file_keyword: String = total_file_record[keyword_index].to_owned();
                total_file_hashmap.insert(total_file_keyword, total_file_record);
            },
            Err(_) => eprintln!("could not read total file records"),
        }
    }

    let mut rdr_file = csv::ReaderBuilder::new().has_headers(header).from_path(file_path).unwrap();
    for file_record in rdr_file.records() {
        match file_record {
            Ok(file_record) => {
                let file_keyword: String = file_record[keyword_index].to_owned();
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

/// 用于多个文件之间并集
/// 输出并集文件
/// 参数说明：
/// file_path:多个文件路径组成的vector, target_file_path：最终生成文件的路径；keyword_index：指定要进行对比的关键字所在的列数，从0开始;
/// header：csv文件中是否含有标题行
fn multiple_file_union(file_path: Vec<PathBuf>, target_file_path: &PathBuf, keyword_index: usize, header: bool) -> i32 {
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

    count
}

/// 用于多个文件之间交集
/// 输出交集文件
/// file_path:多个文件路径组成的vector, target_file_path：最终生成文件的路径；keyword_index：指定要进行对比的关键字所在的列数，从0开始;
/// header：csv文件中是否含有标题行
fn multiple_file_intersection(file_path: Vec<PathBuf>, target_file_path: &PathBuf, keyword_index: usize, header: bool) -> i32 {
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
                let file_keyword: String = first_file_record[keyword_index].to_owned();
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
                    let file_keyword: String = file_record[keyword_index].to_owned();
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

/// 指定关键字在指定的文件中进行搜索
/// 输出搜索结果文件
/// 参数说明：
/// keyword: 支持单个或多个关键字组成的vector，file_path：指定的源数据文件；target_file_path:最终生成的文件路径
/// header:csv文件中是否含有标题行；search_index: 要进行搜索时列数，从0开始
fn search_keyword(keyword: Vec<&str>, file_path: &PathBuf, target_file_path: &PathBuf, header: bool, search_index: usize) -> u16 {
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
                    if query.contains(keyword[i]) {
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

fn main() {
    // 构造命令行参数及使用
    let app = App::new("handle_csv")
            .version("1.0.0")
            .author("likongyang <likongyang18@gmail.com>")
            .about("handle csv file such as files complementary set, union, intersection, merge and search keyword")
            .arg(Arg::with_name("complementary").short("c").long("complementary").takes_value(true).number_of_values(3)
                .help("specify three file and target file path, ordering is file, total file, target file path"))
            .arg(Arg::with_name("union").short("u").long("union").takes_value(true).max_values(200)
                .help("specify files and target file path, ordering is files, target file path"))
            .arg(Arg::with_name("intersection").long("intersection").short("i").takes_value(true).max_values(200)
                .help("specify files and target file path, ordering is files, target file path"))
            .arg(Arg::with_name("merge").long("merge").short("m").takes_value(true).max_values(200)
                .help("spcify which column is the basement"))
            .arg(Arg::with_name("search").long("search").short("s").takes_value(true)
                .help("spcify keywords"))
            .get_matches();


    // 获取每个命令对应的参数输入
    let complementary: Vec<&str> = app.values_of("complementary").unwrap().collect();
    let union: Vec<&str> = app.values_of("union").unwrap().collect();
    let intersction: Vec<&str> = app.values_of("multiple").unwrap().collect();
    let merge: Vec<&str> = app.values_of("merge").unwrap().collect();
    let search: Vec<&str> = app.values_of("search").unwrap().collect();

    // todo：怎么样同时能够通过命令各自输入对应的参数

    // // 测试文件路径
    // let file_one_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/company_data_include_travel.csv");
    // // let file_two_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/data_20201126.csv");
    // let total_file_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/company_data_final_20201126.csv");
    // let target_file_path = PathBuf::from("/Users/likongyang/Desktop/wenlvdianxiao/data/company_data_not_include_travel.csv");
    // // let file_path_vec = vec![file_one_path, file_two_path];

    // // 函数开始运行时间
    // let start_time = std::time::Instant::now();


    // // 搜索关键字
    // // let keyword = vec!["旅行社"];

    // // 测试补集
    // let count = file_complementary_set(&file_one_path, &total_file_path, &target_file_path, 0, true);

    // // 测试多个文件之间的并集
    // // let count = multiple_file_union(file_path_vec, &target_file_path, 0, true);

    // // 测试搜索指定的文件
    // // let count = search_keyword(keyword, &total_file_path, &target_file_path, true, 0);

    // // 测试交集
    // // let count = multiple_file_intersection(file_path_vec, &target_file_path, 0, false);

    // let end_time = std::time::Instant::now();
    // let cost_time = end_time.duration_since(start_time);
    // println!("耗时{:?}", cost_time);

    // println!("一共有 {} 条数据生成", count);
}
