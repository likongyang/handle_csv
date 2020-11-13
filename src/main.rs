use std::path::PathBuf;

/// 用于单个文件和一个汇总文件之间的去重
/// 输出汇总文件中不包含单个文件条目中的文件
/// 参数说明：
/// file_path: 单个文件; total_file_path: 汇总文件； target_file_path: 要生成的文件路径；keyword_index：指定要进行对比的关键字所在的列数，从0开始;
/// header：csv文件中是否含有标题行
fn single_file_unrepeat(file_path: &PathBuf, total_file_path: &PathBuf, target_file_path: &PathBuf, keyword_index: usize, header: bool) -> i32 {
    println!("进入单个文件和汇总的函数中");
    // 生产目标输出文件的写指针、单个文件、汇总文件的指针
    let mut wtr = csv::WriterBuilder::new().has_headers(header).from_path(&target_file_path).unwrap();
    let mut rdr_total_file = csv::ReaderBuilder::new().has_headers(header).from_path(&total_file_path).unwrap();

    // 写入标志及计数器，默认-1则没有任何写入
    let mut write_flag: bool;
    let mut count = 0;

    // 如果文件中存在标题行，则处理标题行
    if header {
        wtr.write_record(rdr_total_file.headers().unwrap()).unwrap();
        wtr.flush().unwrap();
    }

    // 去重核心逻辑
    // todo: 当前为O(N²)的速度，优化为O(logN)
    for total_file_record in rdr_total_file.records() {
        write_flag = true;
        match total_file_record {
            Ok(total_file_record) => {
                let total_file_keyword: &str = &total_file_record[keyword_index];
                let mut rdr_file = csv::ReaderBuilder::new().has_headers(header).from_path(file_path).unwrap();
                for file_record in rdr_file.records() {
                    match file_record {
                        Ok(file_record) => {
                            let file_keyword: &str = &file_record[keyword_index];
                            if file_keyword == total_file_keyword {
                                write_flag = false;
                                break;
                            }
                        },
                        Err(_) => eprintln!("could not read total file records"),
                    }
                }
                if write_flag {
                    wtr.write_record(&total_file_record).unwrap();
                    count += 1;
                }
                
            },
            Err(_) =>eprintln!("could not get keyword"),
        }
    }
    wtr.flush().unwrap();
    println!("完成单个文件的对比");
    count
}

/// 用于多个文件之间交集
/// 输出交集文件
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
    // 以第一个文件作为基础，生成目标文件
    // 从第二个文件开始，跟目标文件进行对比，如果对比没有该条记录，则把这一条记录写入目标文件当中
    for first_file_record in rdr_first_file.records() {
        match first_file_record {
            Ok(first_file_record) => {
                wtr.write_record(&first_file_record).unwrap();
                count += 1;
            },
            Err(_) => eprintln!("write first file failed"),
        }
        
    }
    wtr.flush().unwrap();

    // 部分与单个文件有类似的地方，就是比对的顺序不一致，以及由于单个文件的重新生成文件指针导致创建空白文件
    // 尝试找出是否可以append或者只提炼比较的地方代码生成函数进行调用
    for file_index in 1..file_path.len() {
        let mut rdr_file = csv::ReaderBuilder::new().has_headers(header).from_path(&file_path[file_index]).unwrap();

        for file_record in rdr_file.records() {
            write_flag = true;
            match file_record {
                Ok(file_record) => {
                    let file_keyword: &str = &file_record[keyword_index];
                    let mut rdr_target_file = csv::ReaderBuilder::new().has_headers(header).from_path(target_file_path).unwrap();
                    for target_file_record in rdr_target_file.records() {
                        match target_file_record {
                            Ok(target_file_record) => {
                                let target_file_keyword: &str = &target_file_record[keyword_index];
                                if file_keyword == target_file_keyword {
                                    write_flag = false;
                                    break;
                                }
                            },
                            Err(_) => eprintln!("could not read total file records"),
                        }
                    }
                    if write_flag {
                        wtr.write_record(&file_record).unwrap();
                        count += 1;
                    }
                },
                Err(_) =>eprintln!("could not get keyword"),
            }
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
    // 测试文件路径
    let file_one_path = PathBuf::from("/Users/likongyang/Desktop/test_wenlv/chashuju_1.csv");
    // let file_two_path = PathBuf::from("/Users/likongyang/Desktop/test_wenlv/chashuju_2.csv");
    // let total_file_path = PathBuf::from("/Users/likongyang/Desktop/test_wenlv/total_file.csv");
    let target_file_path = PathBuf::from("/Users/likongyang/Desktop/test_wenlv/target_file.csv");
    // let file_path_vec = vec![file_one_path, file_two_path];

    // 搜索关键字
    let keyword = vec!["测试", "test"];

    // 测试单个文件去重
    // let account = single_file_unrepeat(&file_one_path, &total_file_path, &target_file_path, 0, false);

    // 测试多个文件之间的并集
    // let count = multiple_file_cross_unrepeat(file_path_vec, &target_file_path, 0, false);

    // 测试搜索指定的文件
    let count = search_keyword(keyword, &file_one_path, &target_file_path, false, 20);

    println!("一共有 {} 条数据生成", count);
}
