use std::path::PathBuf;
use std::collections::HashMap;

/// 用于得出单个文件和汇总文件之间的补集
/// 输出汇总文件中不包含单个文件条目中的文件
/// 参数说明：
/// file_path: 单个文件; total_file_path: 汇总文件； target_file_path: 要生成的文件路径；keyword_index：指定要进行对比的关键字所在的列数，从0开始;
/// header：csv文件中是否含有标题行
fn file_complementary_subset(file_path: &PathBuf, total_file_path: &PathBuf, target_file_path: &PathBuf, keyword_index: usize, header: bool) -> i32 {
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
                wtr.write_record(file_record);
                count += 1;
            },
            _ => eprintln!("could not read file record"),
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
fn multiple_file_cross_unrepeat(file_path: Vec<PathBuf>, target_file_path: &PathBuf, keyword_index: usize, header: bool) -> i32 {
    // 计数器，默认-1则没有任何写入
    let mut write_flag: bool;
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
                wtr.write_record(file_record);
                count += 1;
            },
            _ => eprintln!("could not read file record"),
        }
    } 

    wtr.flush().unwrap();

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
                // println!("当前要搜索的关键字是{}", query);
                for i in 0..keyword.len() {
                    if query.contains(keyword[i]) {
                        println!("{}", keyword[i]);
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
    // // 测试文件路径
    let file_one_path = PathBuf::from("/Users/likongyang/Desktop/test_wenlv/chashuju_1.csv");
    let file_two_path = PathBuf::from("/Users/likongyang/Desktop/test_wenlv/chashuju_2.csv");
    // let total_file_path = PathBuf::from("/Users/likongyang/Desktop/test_wenlv/company_data.csv");
    let target_file_path = PathBuf::from("/Users/likongyang/Desktop/test_wenlv/target_file.csv");
    let file_path_vec = vec![file_one_path, file_two_path];

    // // 搜索关键字
    // let keyword = vec!["测试", "test"];

    // // 测试单个文件去重
    let start_time = std::time::Instant::now();
    // let count = file_complementary_subset(&file_one_path, &total_file_path, &target_file_path, 0, true);
    

    // // 测试多个文件之间的并集
    let count = multiple_file_cross_unrepeat(file_path_vec, &target_file_path, 0, false);

    // // 测试搜索指定的文件
    // let count = search_keyword(keyword, &file_one_path, &target_file_path, false, 20);

    let end_time = std::time::Instant::now();
    let cost_time = end_time.duration_since(start_time);
    println!("耗时{:?}", cost_time);

    println!("一共有 {} 条数据生成", count);
}
