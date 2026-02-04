"""
成绩数据处理器
处理Excel文件的读取和数据提取
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


def read_school_data(file_path: str) -> Dict[str, pd.DataFrame]:
    """
    读取【联盟我校数据】Excel文件
    支持两种格式：
    1. 多标签页格式：每个学科一个标签页，包含姓名、班级、得分列
    2. 单表格式：C1D1E1为班级/姓名/学号，F1 J1 N1等为学科名字，F列J列N列为分数
    
    参数:
        file_path: Excel文件路径
        
    返回:
        字典，键为学科名称（如"语文"），值为包含姓名、班级、得分的DataFrame
    """
    try:
        # 读取所有标签页
        excel_file = pd.ExcelFile(file_path)
        sheet_names = excel_file.sheet_names
        
        # 尝试新格式：单表格式（C1D1E1为班级/姓名/学号，F1 J1 N1等为学科名字）
        # 检查第一个标签页是否符合新格式
        try:
            df_test = pd.read_excel(file_path, sheet_name=0, header=0, nrows=5)
            # 检查列名：C列应该是班级，D列应该是姓名，E列应该是学号
            if len(df_test.columns) >= 5:
                col_names = [str(col).strip() for col in df_test.columns[:5]]
                # 检查是否符合新格式：C列包含"班级"，D列包含"姓名"，E列包含"学号"
                is_new_format = False
                if len(col_names) >= 5:
                    c_col = col_names[2] if len(col_names) > 2 else ''
                    d_col = col_names[3] if len(col_names) > 3 else ''
                    e_col = col_names[4] if len(col_names) > 4 else ''
                    
                    if ('班级' in c_col or 'class' in c_col.lower()) and \
                       ('姓名' in d_col or '名字' in d_col or 'name' in d_col.lower()) and \
                       ('学号' in e_col or 'student' in e_col.lower() or 'id' in e_col.lower()):
                        is_new_format = True
                        logger.info("检测到新格式数据：C列为班级，D列为姓名，E列为学号")
                
                if is_new_format:
                    # 读取完整数据
                    df_full = pd.read_excel(file_path, sheet_name=0, header=0)
                    df_full.columns = [str(col).strip() for col in df_full.columns]
                    
                    # 确定列索引
                    class_col_idx = 2  # C列（索引2）
                    name_col_idx = 3   # D列（索引3）
                    student_id_idx = 4 # E列（索引4）
                    
                    # 从F列开始（索引5）查找学科列
                    result = {}
                    subject_cols = {}  # {学科名: 列索引}
                    
                    for idx in range(5, len(df_full.columns)):
                        col_name = str(df_full.columns[idx]).strip()
                        # 跳过没有名称或自动生成的“Unnamed:”列
                        if (not col_name or 
                            col_name.startswith('Unnamed:') or 
                            col_name in ['班级', '姓名', '学号', '得分', '分数', '成绩']):
                            continue
                        # 跳过空列或非学科列
                        # 检查该列是否包含数值数据（可能是学科分数）
                        col_data = df_full.iloc[:, idx]
                        numeric_count = pd.to_numeric(col_data, errors='coerce').notna().sum()
                        if numeric_count > len(col_data) * 0.5:  # 至少50%是数值
                            subject_cols[col_name] = idx
                    
                    logger.info(f"在新格式中检测到 {len(subject_cols)} 个学科: {list(subject_cols.keys())}")
                    
                    # 为每个学科创建DataFrame
                    for subject_name, col_idx in subject_cols.items():
                        try:
                            # 提取班级、姓名、得分
                            subject_df = pd.DataFrame({
                                '班级': df_full.iloc[:, class_col_idx].astype(str).str.strip(),
                                '姓名': df_full.iloc[:, name_col_idx].astype(str).str.strip(),
                                '得分': pd.to_numeric(df_full.iloc[:, col_idx], errors='coerce')
                            })
                            
                            # 清理数据
                            subject_df = subject_df.dropna(subset=['姓名', '得分'])
                            subject_df = subject_df[subject_df['得分'].notna()]
                            
                            # 处理班级列：去除NaN、'nan'等
                            subject_df['班级'] = subject_df['班级'].replace(['nan', 'None', ''], '')
                            
                            if len(subject_df) > 0:
                                result[subject_name] = subject_df
                                avg_score = subject_df['得分'].mean()
                                logger.info(f"✓ 成功读取学科 {subject_name}，共 {len(subject_df)} 条记录，平均分={avg_score:.2f}")
                        except Exception as e:
                            logger.error(f"✗ 处理学科 {subject_name} 时出错: {str(e)}", exc_info=True)
                            continue
                    
                    if len(result) > 0:
                        logger.info(f"新格式读取完成，共 {len(result)} 个学科: {list(result.keys())}")
                        return result
        except Exception as e:
            logger.info(f"尝试新格式失败，使用旧格式: {str(e)}")
        
        # 旧格式：多标签页格式
        # 排除"总分"标签页
        subjects = [name for name in sheet_names if name != '总分']
        
        result = {}
        
        logger.info(f"使用旧格式读取，共有 {len(subjects)} 个学科标签页: {subjects}")
        
        for subject in subjects:
            try:
                # 读取标签页：第一行是空行，第二行是列名（姓名、班级、得分）
                # 使用header=1表示第二行作为列名
                df = pd.read_excel(file_path, sheet_name=subject, header=1)
                
                # 处理MultiIndex列名：如果是MultiIndex，取最后一级
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(-1)
                
                # 将列名转换为字符串，去除前后空格
                df.columns = [str(col).strip() for col in df.columns]
                
                # 标准化列名：查找包含关键字的列
                col_mapping = {}
                name_col = None
                class_col = None
                score_col = None
                
                for col in df.columns:
                    col_str = str(col).strip()
                    if ('姓名' in col_str or '名字' in col_str or 'name' in col_str.lower()) and name_col is None:
                        col_mapping[col] = '姓名'
                        name_col = col
                    elif ('班级' in col_str or 'class' in col_str.lower()) and class_col is None:
                        col_mapping[col] = '班级'
                        class_col = col
                    elif ('得分' in col_str or '分数' in col_str or '成绩' in col_str or 'score' in col_str.lower()) and score_col is None:
                        col_mapping[col] = '得分'
                        score_col = col
                
                # 重命名列
                if col_mapping:
                    df = df.rename(columns=col_mapping)
                
                # 清理数据：去除空行
                if '姓名' in df.columns and '得分' in df.columns:
                    df = df.dropna(subset=['姓名', '得分'])
                    
                    # 确保得分是数值类型
                    df['得分'] = pd.to_numeric(df['得分'], errors='coerce')
                    df = df.dropna(subset=['得分'])
                    
                    # 确保班级是字符串类型
                    if '班级' in df.columns:
                        df['班级'] = df['班级'].astype(str)
                    
                    if len(df) > 0:  # 只添加有数据的学科
                        result[subject] = df
                        # 打印读取到的数据示例
                        avg_score = df['得分'].mean() if '得分' in df.columns else 0
                        logger.info(f"✓ 成功读取学科 {subject}，共 {len(df)} 条记录，平均分={avg_score:.2f}")
                        # 打印前3条记录用于验证
                        if len(df) > 0:
                            sample = df[['姓名', '得分']].head(3) if '姓名' in df.columns else df[['得分']].head(3)
                            logger.info(f"  前3条记录示例:\n{sample.to_string()}")
                    else:
                        logger.warning(f"✗ 学科 {subject} 没有有效数据")
                else:
                    logger.warning(f"✗ 学科 {subject} 的列名格式不正确，实际列名: {list(df.columns)}")
            except Exception as e:
                logger.error(f"✗ 读取学科 {subject} 时出错: {str(e)}", exc_info=True)
                continue
        
        logger.info(f"最终读取到 {len(result)} 个学科: {list(result.keys())}")
        return result
    except Exception as e:
        logger.error(f"读取我校数据文件失败: {str(e)}")
        raise


def build_school_data_from_league(league_df: pd.DataFrame, school_name: str) -> Dict[str, pd.DataFrame]:
    """
    从联盟总成绩数据中，根据“学校”字段提取我校数据，并构造成与 read_school_data 相同结构。

    参数:
        league_df: 通过 read_league_data 读取的联盟总成绩 DataFrame
        school_name: 我校名称（用于匹配“学校”列）

    返回:
        字典，键为学科名称（如"语文"），值为包含姓名、班级、得分的 DataFrame
    """
    result: Dict[str, pd.DataFrame] = {}

    if league_df is None or league_df.empty:
        logger.warning("build_school_data_from_league: 联盟数据为空")
        return result

    if not school_name:
        logger.warning("build_school_data_from_league: 学校名称为空，无法从联盟数据中筛选")
        return result

    if '学校' not in league_df.columns:
        logger.warning("build_school_data_from_league: 联盟数据中没有“学校”列，无法筛选我校数据")
        return result

    # 只做精确匹配（用户保证能提供精确学校名称）
    school_series = league_df['学校'].astype(str)
    school_df = league_df[school_series == school_name].copy()

    if school_df.empty:
        logger.warning(
            f"build_school_data_from_league: 使用学校名称 '{school_name}' "
            f"在联盟数据中没有筛选到任何记录"
        )
        return result

    logger.info(
        f"build_school_data_from_league: 使用学校名称 '{school_name}' "
        f"从联盟数据中筛选到 {len(school_df)} 条记录"
    )

    # 确保姓名、班级为字符串
    if '姓名' in school_df.columns:
        school_df['姓名'] = school_df['姓名'].astype(str).str.strip()
    if '班级' in school_df.columns:
        school_df['班级'] = school_df['班级'].astype(str).str.strip()

    # 按科目列拆成与 read_school_data 一致的结构
    score_columns = ['语文', '数学', '英语', '物理', '化学', '生物', '政治', '历史', '地理']
    available_subjects = [col for col in score_columns if col in school_df.columns]

    if not available_subjects:
        logger.warning("build_school_data_from_league: 在联盟数据中没有找到任何学科列")
        return result

    for subject in available_subjects:
        try:
            df_sub = pd.DataFrame()
            if '班级' in school_df.columns:
                df_sub['班级'] = school_df['班级']
            else:
                # 如果没有班级列，也要保证字段存在，后续逻辑依赖
                df_sub['班级'] = ''

            if '姓名' in school_df.columns:
                df_sub['姓名'] = school_df['姓名']
            else:
                df_sub['姓名'] = ''

            df_sub['得分'] = pd.to_numeric(school_df[subject], errors='coerce')

            # 清理无效数据
            df_sub = df_sub.dropna(subset=['得分'])
            df_sub = df_sub[df_sub['得分'].notna()]

            if len(df_sub) == 0:
                logger.warning(f"build_school_data_from_league: 学科 {subject} 在我校数据中没有有效成绩记录，跳过")
                continue

            result[subject] = df_sub
            logger.info(
                f"build_school_data_from_league: 学科 {subject} 生成 {len(df_sub)} 条我校记录，"
                f"平均分={df_sub['得分'].mean():.2f}"
            )
        except Exception as e:
            logger.error(f"build_school_data_from_league: 处理学科 {subject} 时出错: {str(e)}", exc_info=True)
            continue

    logger.info(
        f"build_school_data_from_league: 共为我校生成 {len(result)} 个学科的数据结构，"
        f"学科列表: {list(result.keys())}"
    )
    return result


def read_league_data(file_path: str) -> pd.DataFrame:
    """
    读取【联盟全体数据】Excel文件的"分数"标签页
    
    参数:
        file_path: Excel文件路径
        
    返回:
        包含学校、姓名、班级和各科成绩的DataFrame
    """
    try:
        # 读取"分数"标签页
        df = pd.read_excel(file_path, sheet_name='分数')
        
        # 处理MultiIndex列名：如果是MultiIndex，取最后一级
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(-1)
        
        # 将列名转换为字符串，去除前后空格
        df.columns = [str(col).strip() for col in df.columns]
        
        logger.info(f"联盟数据原始列名: {list(df.columns)}")
        
        # 预期的列名
        expected_columns = ['学校', '姓名', '班级', '语文', '数学', '英语', 
                          '物理', '化学', '生物', '政治', '历史', '地理']
        
        # 标准化列名映射
        rename_map = {}
        for actual_col in df.columns:
            actual_col_str = str(actual_col).strip()
            for expected in expected_columns:
                # 精确匹配或包含匹配
                if actual_col_str == expected or expected in actual_col_str:
                    rename_map[actual_col] = expected
                    break
        
        # 重命名列
        if rename_map:
            df = df.rename(columns=rename_map)
            logger.info(f"联盟数据重命名后的列名: {list(df.columns)}")
        
        # 只保留预期的列（如果存在）
        available_columns = [col for col in expected_columns if col in df.columns]
        if available_columns:
            df = df[available_columns].copy()
        else:
            logger.warning("没有找到任何预期的列，使用所有列")
        
        # 确保学校、姓名、班级是字符串类型
        if '学校' in df.columns:
            df['学校'] = df['学校'].astype(str)
        if '姓名' in df.columns:
            df['姓名'] = df['姓名'].astype(str)
        if '班级' in df.columns:
            df['班级'] = df['班级'].astype(str)
        
        # 将各科成绩转换为数值类型
        score_columns = ['语文', '数学', '英语', '物理', '化学', '生物', '政治', '历史', '地理']
        for col in score_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 去除所有成绩都为空的行
        score_cols = [col for col in score_columns if col in df.columns]
        if score_cols:
            df = df.dropna(subset=score_cols, how='all')
        
        logger.info(f"成功读取联盟全体数据，共 {len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"读取联盟全体数据文件失败: {str(e)}")
        raise


def analyze_school_scores(school_data: Dict[str, pd.DataFrame], score_lines: List[float]) -> Dict:
    """
    分析我校成绩数据（需求1）
    注意：单科分数不进行分数线分析，因为特控线、一段线等是总分线
    
    参数:
        school_data: 我校各学科数据字典
        score_lines: 分数线列表（此参数保留但不使用，因为单科不分析分数线）
    
    返回:
        分析结果字典
    """
    results = {}
    
    # 为每个学科进行分析
    for subject, df in school_data.items():
        if df.empty:
            logger.warning(f"学科 {subject} 的数据为空，但仍返回空数据结构")
            # 即使数据为空，也返回空数据结构，确保前端能显示该学科
            results[subject] = {
                'total_students': 0,
                'average_score': 0,
                'max_score': 0,
                'min_score': 0,
                'median_score': 0,
                'std_score': 0,
                'class_stats': {}
            }
            continue
        
        subject_results = {}
        
        # 基本统计
        total_students = len(df)
        average_score = float(df['得分'].mean())
        max_score = float(df['得分'].max())
        min_score = float(df['得分'].min())
        median_score = float(df['得分'].median())
        std_score = float(df['得分'].std())
        
        # 打印每个学科的平均成绩和学生数量
        logger.info(f"学科 {subject}: 学生数={total_students}, 平均分={average_score:.2f}, 最高分={max_score:.2f}, 最低分={min_score:.2f}, 中位数={median_score:.2f}")
        
        # 打印前5个学生的成绩（用于验证数据读取）
        if total_students > 0:
            sample_students = df[['姓名', '得分']].head(5) if '姓名' in df.columns else df[['得分']].head(5)
            logger.info(f"学科 {subject} 前5个学生成绩示例:\n{sample_students.to_string()}")
        
        subject_results['total_students'] = total_students
        subject_results['average_score'] = average_score
        subject_results['max_score'] = max_score
        subject_results['min_score'] = min_score
        subject_results['median_score'] = median_score
        subject_results['std_score'] = std_score
        
        # 按班级统计平均分
        class_stats = {}
        if '班级' in df.columns:
            try:
                for class_name in df['班级'].dropna().unique():
                    class_df = df[df['班级'] == class_name]
                    class_stats[str(class_name)] = {
                        'count': int(len(class_df)),
                        'average': round(float(class_df['得分'].mean()), 2),
                        'max': round(float(class_df['得分'].max()), 2),
                        'min': round(float(class_df['得分'].min()), 2)
                    }
            except Exception as e:
                logger.warning(f"统计班级数据时出错: {str(e)}")
                class_stats = {}
        
        subject_results['class_stats'] = class_stats
        
        # 不进行分数线分析（因为特控线、一段线等是总分线，不适用于单科）
        results[subject] = subject_results
    
    return results


def analyze_school_subjects_by_class(school_data: Dict[str, pd.DataFrame], score_lines: List[float] = None) -> Dict:
    """
    分析我校各学科成绩，按班级横向对比
    
    参数:
        school_data: 我校各学科数据字典
        score_lines: 分数线列表（保留参数以兼容，但不使用）
    
    返回:
        分析结果字典，按学科组织，每个学科包含各班级的统计信息
    """
    results = {}
    
    if not school_data:
        logger.warning("我校数据为空，无法进行学科分析")
        return results
    
    logger.info(f"开始分析各学科班级对比，共 {len(school_data)} 个学科")
    
    # 分析每个学科
    for subject, df in school_data.items():
        if df.empty:
            results[subject] = {
                'total_students': 0,
                'average_score': 0,
                'max_score': 0,
                'min_score': 0,
                'median_score': 0,
                'std_score': 0,
                'class_stats': []
            }
            continue
        
        subject_results = {}
        
        # 基本统计
        total_students = len(df)
        average_score = float(df['得分'].mean())
        max_score = float(df['得分'].max())
        min_score = float(df['得分'].min())
        median_score = float(df['得分'].median())
        std_score = float(df['得分'].std())
        
        subject_results['total_students'] = total_students
        subject_results['average_score'] = round(average_score, 2)
        subject_results['max_score'] = round(max_score, 2)
        subject_results['min_score'] = round(min_score, 2)
        subject_results['median_score'] = round(median_score, 2)
        subject_results['std_score'] = round(std_score, 2)
        
        # 按班级统计
        class_stats = []
        if '班级' in df.columns:
            try:
                # 处理班级列
                df_copy = df.copy()
                df_copy['班级'] = df_copy['班级'].astype(str).str.strip()
                df_copy['班级'] = df_copy['班级'].replace(['nan', 'None', ''], '')
                
                # 获取有效班级
                valid_classes = df_copy['班级'].dropna()
                valid_classes = valid_classes[valid_classes.astype(str).str.strip() != '']
                valid_classes = valid_classes[valid_classes.astype(str) != 'nan']
                unique_classes = valid_classes.unique()
                
                for class_name in unique_classes:
                    class_name_str = str(class_name).strip()
                    if not class_name_str or class_name_str == 'nan':
                        continue
                    
                    class_df = df_copy[df_copy['班级'].astype(str).str.strip() == class_name_str]
                    
                    class_total = len(class_df)
                    class_avg = float(class_df['得分'].mean()) if class_total > 0 else 0
                    class_max = float(class_df['得分'].max()) if class_total > 0 else 0
                    class_min = float(class_df['得分'].min()) if class_total > 0 else 0
                    class_median = float(class_df['得分'].median()) if class_total > 0 else 0
                    
                    class_stat = {
                        'class_name': class_name_str,
                        'total_students': int(class_total),
                        'average_score': round(class_avg, 2),
                        'max_score': round(class_max, 2),
                        'min_score': round(class_min, 2),
                        'median_score': round(class_median, 2)
                    }
                    
                    class_stats.append(class_stat)
                
                # 按平均分排序
                class_stats.sort(key=lambda x: x['average_score'], reverse=True)
                logger.info(f"学科 {subject}: 统计了 {len(class_stats)} 个班级")
                
            except Exception as e:
                logger.warning(f"统计学科 {subject} 的班级数据时出错: {str(e)}", exc_info=True)
                class_stats = []
        else:
            logger.warning(f"学科 {subject} 没有班级列，跳过班级统计")
            class_stats = []
        
        subject_results['class_stats'] = class_stats
        results[subject] = subject_results
    
    logger.info(f"各学科分析完成，共 {len(results)} 个学科")
    return results


def analyze_school_total_score(school_data: Dict[str, pd.DataFrame], score_lines: List[float]) -> Dict:
    """
    分析我校成绩数据的总分和分数线情况（新需求）
    从各学科数据中合并计算总分，然后根据分数线分析特控线率、过线学生数量、平均分等
    
    数据来源说明：
    - school_data 由 read_school_data() 函数读取得到
    - Excel文件结构：多个以学科名字命名的标签页（如"语文"、"数学"等）
    - 每个标签页的第二行是列标题，包含：姓名、班级、得分
    - read_school_data() 已处理列名标准化和数据清理
    
    参数:
        school_data: 我校各学科数据字典，键为学科名称，值为包含姓名、班级、得分的DataFrame
        score_lines: 分数线列表
    
    返回:
        分析结果字典，按分数线组织，每个分数线包含：
        - 基本统计：总人数、平均分、最高分、最低分、中位数、标准差
        - 过线情况：过线人数、过线率、过线学生平均分、未过线学生平均分
        - 班级统计：各班级过线情况详情
        - 班级分布：过线学生的班级分布
    """
    results = {}
    
    if not school_data:
        logger.warning("我校数据为空，无法进行总分分析")
        return results
    
    # 合并所有学科数据，计算总分
    # 使用姓名和班级作为唯一标识
    # 数据结构：每个学科标签页的第二行是列标题（姓名、班级、得分）
    total_score_df = None
    has_class_column = False  # 跟踪是否有班级列
    
    logger.info(f"开始合并 {len(school_data)} 个学科的数据计算总分")
    
    for subject, df in school_data.items():
        if df.empty:
            logger.warning(f"学科 {subject} 数据为空，跳过")
            continue
        
        # 确保有必要的列（基于read_school_data的读取逻辑，应该有姓名和得分）
        if '姓名' not in df.columns or '得分' not in df.columns:
            logger.warning(f"学科 {subject} 缺少必要列（姓名或得分），实际列名: {list(df.columns)}，跳过")
            continue
        
        # 创建临时DataFrame，包含姓名、班级和该学科得分
        subject_df = df[['姓名', '得分']].copy()
        
        # 处理班级列：统一处理，确保数据类型一致
        if '班级' in df.columns:
            subject_df['班级'] = df['班级'].astype(str).str.strip()  # 转换为字符串并去除空格
            # 将NaN、'nan'、空字符串统一处理
            subject_df['班级'] = subject_df['班级'].replace(['nan', 'None', ''], '')
            has_class_column = True
        else:
            subject_df['班级'] = ''
        
        # 确保姓名也是字符串类型，并去除空格
        subject_df['姓名'] = subject_df['姓名'].astype(str).str.strip()
        
        # 确保得分是数值类型
        subject_df['得分'] = pd.to_numeric(subject_df['得分'], errors='coerce')
        
        # 去除得分为NaN的行（无效数据）
        subject_df = subject_df.dropna(subset=['得分'])
        
        if len(subject_df) == 0:
            logger.warning(f"学科 {subject} 没有有效得分数据，跳过")
            continue
        
        # 重命名得分列为学科名
        subject_df = subject_df.rename(columns={'得分': subject})
        
        logger.info(f"学科 {subject}: {len(subject_df)} 条有效记录，平均分={subject_df[subject].mean():.2f}")
        
        # 合并到总分DataFrame
        if total_score_df is None:
            total_score_df = subject_df
            logger.info(f"初始化总分DataFrame，使用学科 {subject}，共 {len(total_score_df)} 条记录")
        else:
            # 按姓名和班级合并（如果都有班级列）
            merge_keys = ['姓名']
            if has_class_column and '班级' in total_score_df.columns and '班级' in subject_df.columns:
                merge_keys.append('班级')
                logger.debug(f"按姓名和班级合并学科 {subject}")
            else:
                logger.debug(f"按姓名合并学科 {subject}（班级列不一致或缺失）")
            
            # 使用outer join确保包含所有学生（即使某个学生在某个学科没有成绩）
            total_score_df = pd.merge(
                total_score_df, 
                subject_df, 
                on=merge_keys, 
                how='outer'
            )
            logger.info(f"合并后共 {len(total_score_df)} 条记录")
    
    if total_score_df is None or total_score_df.empty:
        logger.warning("无法合并学科数据计算总分：没有有效数据")
        return results
    
    # 计算总分（所有学科得分相加，忽略NaN）
    # 排除姓名和班级列，以及任何名为“总分”或包含“总分”的列，防止把原始总分列当作一个学科再加一次
    score_columns = [
        col for col in total_score_df.columns
        if col not in ['姓名', '班级']
        and '总分' not in str(col)
    ]
    logger.info(f"计算总分，参与计算的学科: {score_columns}")
    
    total_score_df['总分'] = total_score_df[score_columns].sum(axis=1, skipna=True)
    
    # 去除总分为NaN或0的行（这些学生可能所有学科都没有成绩）
    before_filter = len(total_score_df)
    total_score_df = total_score_df[total_score_df['总分'].notna() & (total_score_df['总分'] > 0)]
    after_filter = len(total_score_df)
    
    if before_filter > after_filter:
        logger.info(f"过滤无效数据：{before_filter} -> {after_filter} 条记录")
    
    if total_score_df.empty:
        logger.warning("计算总分后没有有效数据")
        return results
    
    logger.info(f"成功合并学科数据，共 {len(total_score_df)} 名学生，总分范围: {total_score_df['总分'].min():.2f} - {total_score_df['总分'].max():.2f}")
    logger.info(f"参与计算的学科数量: {len(score_columns)}，学科列表: {score_columns}")
    
    # 按分数线分析
    for line in score_lines:
        line_results = {}
        
        # 基本统计
        total_students = len(total_score_df)
        average_score = float(total_score_df['总分'].mean())
        max_score = float(total_score_df['总分'].max())
        min_score = float(total_score_df['总分'].min())
        median_score = float(total_score_df['总分'].median())
        std_score = float(total_score_df['总分'].std())
        
        # 过线情况
        passed_df = total_score_df[total_score_df['总分'] >= line]
        passed_count = len(passed_df)
        pass_rate = (passed_count / total_students * 100) if total_students > 0 else 0
        
        # 过线学生平均分
        passed_avg_score = float(passed_df['总分'].mean()) if passed_count > 0 else 0
        
        # 未过线学生平均分
        not_passed_df = total_score_df[total_score_df['总分'] < line]
        not_passed_count = len(not_passed_df)
        not_passed_avg_score = float(not_passed_df['总分'].mean()) if not_passed_count > 0 else 0
        
        line_results['score_line'] = float(line)
        line_results['total_students'] = int(total_students)
        line_results['average_score'] = round(average_score, 2)
        line_results['max_score'] = round(max_score, 2)
        line_results['min_score'] = round(min_score, 2)
        line_results['median_score'] = round(median_score, 2)
        line_results['std_score'] = round(std_score, 2)
        line_results['passed_count'] = int(passed_count)
        line_results['pass_rate'] = round(pass_rate, 2)
        line_results['passed_avg_score'] = round(passed_avg_score, 2)
        line_results['not_passed_count'] = int(not_passed_count)
        line_results['not_passed_avg_score'] = round(not_passed_avg_score, 2)
        
        # 按班级统计
        class_stats = []
        if '班级' in total_score_df.columns:
            try:
                # 获取所有非空的班级名称（排除空字符串、'nan'等）
                valid_classes = total_score_df['班级'].dropna()
                valid_classes = valid_classes[valid_classes.astype(str).str.strip() != '']
                valid_classes = valid_classes[valid_classes.astype(str) != 'nan']
                unique_classes = valid_classes.unique()
                
                if len(unique_classes) > 0:
                    for class_name in unique_classes:
                        class_name_str = str(class_name).strip()
                        if not class_name_str or class_name_str == 'nan':
                            continue
                            
                        class_df = total_score_df[total_score_df['班级'].astype(str).str.strip() == class_name_str]
                        class_passed = class_df[class_df['总分'] >= line]
                        
                        class_total = len(class_df)
                        class_passed_count = len(class_passed)
                        class_pass_rate = (class_passed_count / class_total * 100) if class_total > 0 else 0
                        class_avg_score = float(class_df['总分'].mean()) if class_total > 0 else 0
                        class_passed_avg = float(class_passed['总分'].mean()) if class_passed_count > 0 else 0
                        
                        class_stats.append({
                            'class_name': class_name_str,
                            'total_students': int(class_total),
                            'passed_count': int(class_passed_count),
                            'pass_rate': round(class_pass_rate, 2),
                            'average_score': round(class_avg_score, 2),
                            'passed_avg_score': round(class_passed_avg, 2)
                        })
                    
                    # 按过线率排序
                    class_stats.sort(key=lambda x: x['pass_rate'], reverse=True)
                    logger.info(f"分数线 {line} 分：统计了 {len(class_stats)} 个班级的数据")
                else:
                    logger.warning(f"没有找到有效的班级数据")
            except Exception as e:
                logger.warning(f"统计班级数据时出错: {str(e)}", exc_info=True)
                class_stats = []
        
        line_results['class_stats'] = class_stats
        
        # 过线学生班级分布
        class_distribution = {}
        if not passed_df.empty and '班级' in passed_df.columns:
            try:
                # 过滤有效的班级数据（排除空字符串、'nan'等）
                class_series = passed_df['班级'].dropna().astype(str).str.strip()
                class_series = class_series[class_series != '']
                class_series = class_series[class_series != 'nan']
                
                if len(class_series) > 0:
                    class_counts = class_series.value_counts().to_dict()
                    class_distribution = {str(k).strip(): int(v) for k, v in class_counts.items() if str(k).strip()}
            except Exception as e:
                logger.warning(f"统计过线学生班级分布时出错: {str(e)}")
                class_distribution = {}
        
        line_results['class_distribution'] = class_distribution
        
        results[f'line_{line}'] = line_results
    
    return results


def analyze_league_scores(league_df: pd.DataFrame, school_name: str, score_lines: List[float], display_name: str = None) -> Dict:
    """
    分析联盟全体成绩数据（需求2）
    
    参数:
        league_df: 联盟全体数据DataFrame
        school_name: 我校名称（用于筛选）
        score_lines: 分数线列表
        
    返回:
        分析结果字典
    """
    results = {}
    
    # 计算总分（所有科目相加）
    score_columns = ['语文', '数学', '英语', '物理', '化学', '生物', '政治', '历史', '地理']
    available_score_cols = [col for col in score_columns if col in league_df.columns]
    
    if not available_score_cols:
        logger.warning("没有找到任何成绩列")
        return results
    
    # 计算总分（忽略NaN值）
    league_df = league_df.copy()
    league_df['总分'] = league_df[available_score_cols].sum(axis=1, skipna=True)
    
    # 去除总分为NaN或0的行
    league_df = league_df[league_df['总分'].notna() & (league_df['总分'] > 0)]
    
    # 筛选我校数据（只做精确匹配，用户保证学校名称准确）
    if '学校' in league_df.columns:
        school_df = league_df[league_df['学校'] == school_name].copy()
        logger.info(f"使用学校名称 '{school_name}' 精确匹配到 {len(school_df)} 条记录")
    else:
        school_df = pd.DataFrame()
    
    # 按分数线分析
    for line in score_lines:
        line_results = {}
        
        # 联盟全体过线情况
        league_passed = league_df[league_df['总分'] >= line]
        league_total = len(league_df)
        league_passed_count = len(league_passed)
        league_pass_rate = (league_passed_count / league_total * 100) if league_total > 0 else 0
        
        # 按学校统计
        school_stats = []
        if '学校' in league_df.columns:
            for school in league_df['学校'].unique():
                school_data = league_df[league_df['学校'] == school]
                school_passed = school_data[school_data['总分'] >= line]
                
                school_stats.append({
                    'school_name': str(school),
                    'total_students': int(len(school_data)),
                    'passed_count': int(len(school_passed)),
                    'pass_rate': round((len(school_passed) / len(school_data) * 100) if len(school_data) > 0 else 0, 2),
                    'average_score': round(float(school_data['总分'].mean()), 2) if len(school_data) > 0 else 0
                })
            
            # 按过线率排序
            school_stats.sort(key=lambda x: x['pass_rate'], reverse=True)
        
        # 我校过线情况
        school_passed = school_df[school_df['总分'] >= line] if not school_df.empty else pd.DataFrame()
        school_passed_count = len(school_passed)
        school_total = len(school_df)
        school_pass_rate = (school_passed_count / school_total * 100) if school_total > 0 else 0
        
        # 我校过线学生班级分布
        school_class_distribution = {}
        if not school_passed.empty and '班级' in school_passed.columns:
            try:
                # 确保班级列是字符串类型，去除NaN值
                class_series = school_passed['班级'].dropna().astype(str)
                if len(class_series) > 0:
                    class_counts = class_series.value_counts().to_dict()
                    school_class_distribution = {str(k): int(v) for k, v in class_counts.items()}
            except Exception as e:
                logger.warning(f"统计班级分布时出错: {str(e)}")
                school_class_distribution = {}
        
        # 我校各班级特控率统计
        school_class_pass_stats = []
        if not school_df.empty and '班级' in school_df.columns:
            try:
                # 按班级统计
                for class_name in school_df['班级'].dropna().unique():
                    class_df = school_df[school_df['班级'] == class_name]
                    class_passed = class_df[class_df['总分'] >= line]
                    
                    class_total = len(class_df)
                    class_passed_count = len(class_passed)
                    class_pass_rate = (class_passed_count / class_total * 100) if class_total > 0 else 0
                    class_avg_score = float(class_df['总分'].mean()) if class_total > 0 else 0
                    
                    school_class_pass_stats.append({
                        'class_name': str(class_name),
                        'total_students': int(class_total),
                        'passed_count': int(class_passed_count),
                        'pass_rate': round(class_pass_rate, 2),
                        'average_score': round(class_avg_score, 2)
                    })
                
                # 按过线率排序
                school_class_pass_stats.sort(key=lambda x: x['pass_rate'], reverse=True)
            except Exception as e:
                logger.warning(f"统计班级特控率时出错: {str(e)}")
                school_class_pass_stats = []
        
        # 我校排名（在联盟中的位置）
        school_rank = None
        display_school_name = display_name if display_name else school_name
        if school_name and not school_df.empty:
            # 找到我校在按过线率排序中的位置
            for idx, stat in enumerate(school_stats, 1):
                if stat['school_name'] == school_name:
                    school_rank = idx
                    break
        
        line_results = {
            'score_line': float(line),
            'league_total': int(league_total),
            'league_passed_count': int(league_passed_count),
            'league_pass_rate': round(league_pass_rate, 2),
            'school_stats': school_stats,
            'school_total': int(school_total),
            'school_passed_count': int(school_passed_count),
            'school_pass_rate': round(school_pass_rate, 2),
            'school_rank': school_rank,
            'school_class_distribution': school_class_distribution,
            'school_class_pass_stats': school_class_pass_stats,  # 新增：各班级特控率统计
            'school_average_score': round(float(school_df['总分'].mean()), 2) if not school_df.empty else 0
        }
        
        results[f'line_{line}'] = line_results
    
    return results


def analyze_subject_score_lines(school_data: Dict[str, pd.DataFrame], 
                                total_score_line: float,
                                subject_score_lines: Dict[str, float]) -> Dict:
    """
    分析各学科的分数线情况
    
    参数:
        school_data: 我校各学科数据字典
        total_score_line: 总分分数线
        subject_score_lines: 各学科分数线字典 {学科名: 分数线}
    
    返回:
        分析结果字典，包含各学科的过线情况
    """
    results = {
        'total_score_line': total_score_line,
        'subject_lines': subject_score_lines,
        'subjects': {}
    }
    
    # 分析每个学科的分数线
    for subject, score_line in subject_score_lines.items():
        if subject not in school_data:
            logger.warning(f"学科 {subject} 不在数据中，跳过")
            continue
        
        df = school_data[subject]
        if df.empty:
            continue
        
        # 基本统计
        total_students = len(df)
        passed_count = len(df[df['得分'] >= score_line])
        pass_rate = (passed_count / total_students * 100) if total_students > 0 else 0
        
        # 按班级统计
        class_stats = []
        if '班级' in df.columns:
            try:
                valid_classes = df['班级'].dropna()
                valid_classes = valid_classes[valid_classes.astype(str).str.strip() != '']
                valid_classes = valid_classes[valid_classes.astype(str) != 'nan']
                unique_classes = valid_classes.unique()
                
                for class_name in unique_classes:
                    class_name_str = str(class_name).strip()
                    if not class_name_str or class_name_str == 'nan':
                        continue
                    
                    class_df = df[df['班级'].astype(str).str.strip() == class_name_str]
                    class_passed = class_df[class_df['得分'] >= score_line]
                    
                    class_total = len(class_df)
                    class_passed_count = len(class_passed)
                    class_pass_rate = (class_passed_count / class_total * 100) if class_total > 0 else 0
                    
                    class_stats.append({
                        'class_name': class_name_str,
                        'total_students': int(class_total),
                        'passed_count': int(class_passed_count),
                        'pass_rate': round(class_pass_rate, 2)
                    })
                
                class_stats.sort(key=lambda x: x['pass_rate'], reverse=True)
            except Exception as e:
                logger.warning(f"统计学科 {subject} 的班级数据时出错: {str(e)}")
        
        results['subjects'][subject] = {
            'score_line': float(score_line),
            'total_students': int(total_students),
            'passed_count': int(passed_count),
            'pass_rate': round(pass_rate, 2),
            'class_stats': class_stats
        }
    
    return results


def analyze_class_subjects_table(school_data: Dict[str, pd.DataFrame],
                                  score_line: float,
                                  subject_score_lines: Dict[str, float] = None) -> Dict:
    """
    分析班级各科情况表格
    纵坐标为班级，横坐标为学科，显示各班的过线人数和过线率
    
    参数:
        school_data: 我校各学科数据字典
        score_line: 总分分数线
        subject_score_lines: 各学科分数线字典（可选）
    
    返回:
        分析结果字典，包含班级×学科的过线情况
    """
    results = {
        'score_line': score_line,
        'subject_lines': subject_score_lines or {},
        'classes': {}  # {班级名: {学科名: {passed_count, pass_rate, total_students}}}
    }
    
    # 获取所有班级和学科
    all_classes = set()
    all_subjects = list(school_data.keys())
    
    for subject, df in school_data.items():
        if '班级' in df.columns:
            valid_classes = df['班级'].dropna()
            valid_classes = valid_classes[valid_classes.astype(str).str.strip() != '']
            valid_classes = valid_classes[valid_classes.astype(str) != 'nan']
            all_classes.update(valid_classes.unique())
    
    all_classes = sorted([str(c).strip() for c in all_classes if str(c).strip() and str(c).strip() != 'nan'])
    
    # 初始化班级数据
    for class_name in all_classes:
        results['classes'][class_name] = {}
    
    # 分析每个学科
    for subject, df in school_data.items():
        if df.empty:
            continue
        
        # 确定该学科的分数线
        subject_line = None
        if subject_score_lines and subject in subject_score_lines:
            subject_line = subject_score_lines[subject]
        
        if subject_line is None:
            continue
        
        # 按班级统计
        if '班级' in df.columns:
            for class_name in all_classes:
                class_df = df[df['班级'].astype(str).str.strip() == class_name]
                if len(class_df) == 0:
                    continue
                
                class_passed = class_df[class_df['得分'] >= subject_line]
                class_total = len(class_df)
                class_passed_count = len(class_passed)
                class_pass_rate = (class_passed_count / class_total * 100) if class_total > 0 else 0
                
                results['classes'][class_name][subject] = {
                    'total_students': int(class_total),
                    'passed_count': int(class_passed_count),
                    'pass_rate': round(class_pass_rate, 2)
                }
    
    return results


def calculate_class_assessment(school_data: Dict[str, pd.DataFrame],
                              tekong_line: float,
                              yiduan_line: float) -> Dict[str, List[Dict]]:
    """
    计算班级考核分
    计算方法：0.3 * 特控率 + 0.7 * 一段率
    
    参数:
        school_data: 我校各学科数据字典
        tekong_line: 特控线（总分）
        yiduan_line: 一段线（总分）
    
    返回:
        班级考核结果列表，按考核分排序
    """
    # 合并所有学科数据，准备用于计算总分
    total_score_df = None
    has_class_column = False
    excluded_students: List[Dict] = []  # 记录缺考或成绩异常的学生
    
    for subject, df in school_data.items():
        if df.empty:
            continue
        
        if '姓名' not in df.columns or '得分' not in df.columns:
            continue
        
        subject_df = df[['姓名', '得分']].copy()
        
        if '班级' in df.columns:
            subject_df['班级'] = df['班级'].astype(str).str.strip()
            subject_df['班级'] = subject_df['班级'].replace(['nan', 'None', ''], '')
            has_class_column = True
        else:
            subject_df['班级'] = ''
        
        subject_df['姓名'] = subject_df['姓名'].astype(str).str.strip()
        subject_df['得分'] = pd.to_numeric(subject_df['得分'], errors='coerce')
        subject_df = subject_df.dropna(subset=['得分'])
        
        if len(subject_df) == 0:
            continue
        
        subject_df = subject_df.rename(columns={'得分': subject})
        
        if total_score_df is None:
            total_score_df = subject_df
        else:
            merge_keys = ['姓名']
            if has_class_column and '班级' in total_score_df.columns and '班级' in subject_df.columns:
                merge_keys.append('班级')
            
            total_score_df = pd.merge(
                total_score_df,
                subject_df,
                on=merge_keys,
                how='outer'
            )
    
    if total_score_df is None or total_score_df.empty:
        return {"class_results": [], "excluded_students": []}
    
    # 如果原始数据中已经存在总分列（列名包含“总分”），优先使用该列
    total_score_cols = [
        col for col in total_score_df.columns
        if col not in ['姓名', '班级'] and '总分' in str(col)
    ]
    
    if total_score_cols:
        total_col = total_score_cols[0]
        total_score_df['总分'] = pd.to_numeric(total_score_df[total_col], errors='coerce')
        
        # 排除总分为空或<=0的学生，记为特殊学生
        invalid_mask = total_score_df['总分'].isna() | (total_score_df['总分'] <= 0)
        excluded_df = total_score_df[invalid_mask]
        for _, row in excluded_df.iterrows():
            excluded_students.append({
                "姓名": str(row.get('姓名', '')).strip(),
                "班级": str(row.get('班级', '')).strip(),
                "原因": "总分缺失或为0"
            })
        
        total_score_df = total_score_df[~invalid_mask].copy()
    else:
        # 没有原始总分列，则通过各科成绩计算总分
        subject_columns = [col for col in total_score_df.columns if col not in ['姓名', '班级']]
        
        if not subject_columns:
            return {"class_results": [], "excluded_students": []}
        
        # 先筛除任一学科成绩缺失的学生（视为缺考）
        valid_mask = total_score_df[subject_columns].notna().all(axis=1)
        excluded_df = total_score_df[~valid_mask]
        for _, row in excluded_df.iterrows():
            excluded_students.append({
                "姓名": str(row.get('姓名', '')).strip(),
                "班级": str(row.get('班级', '')).strip(),
                "原因": "存在缺考或成绩缺失"
            })
        
        total_score_df = total_score_df[valid_mask].copy()
        
        if total_score_df.empty:
            return {"class_results": [], "excluded_students": excluded_students}
        
        # 计算总分（所有学科得分相加）
        total_score_df['总分'] = total_score_df[subject_columns].sum(axis=1, skipna=False)
    
    if total_score_df.empty or '班级' not in total_score_df.columns:
        return {"class_results": [], "excluded_students": excluded_students}
    
    # 按班级统计
    class_results = []
    valid_classes = total_score_df['班级'].dropna()
    valid_classes = valid_classes[valid_classes.astype(str).str.strip() != '']
    valid_classes = valid_classes[valid_classes.astype(str) != 'nan']
    unique_classes = valid_classes.unique()
    
    for class_name in unique_classes:
        class_name_str = str(class_name).strip()
        if not class_name_str or class_name_str == 'nan':
            continue
        
        class_df = total_score_df[total_score_df['班级'].astype(str).str.strip() == class_name_str]
        
        class_total = len(class_df)
        tekong_passed = len(class_df[class_df['总分'] >= tekong_line])
        yiduan_passed = len(class_df[class_df['总分'] >= yiduan_line])
        
        tekong_rate = (tekong_passed / class_total * 100) if class_total > 0 else 0
        yiduan_rate = (yiduan_passed / class_total * 100) if class_total > 0 else 0
        
        # 计算考核分
        assessment_score = 0.3 * tekong_rate + 0.7 * yiduan_rate
        
        class_results.append({
            'class_name': class_name_str,
            'total_students': int(class_total),
            'tekong_passed': int(tekong_passed),
            'tekong_rate': round(tekong_rate, 2),
            'yiduan_passed': int(yiduan_passed),
            'yiduan_rate': round(yiduan_rate, 2),
            'assessment_score': round(assessment_score, 2)
        })
    
    # 按考核分排序
    class_results.sort(key=lambda x: x['assessment_score'], reverse=True)
    
    # 添加排名
    for idx, result in enumerate(class_results, 1):
        result['rank'] = idx
    
    return {"class_results": class_results, "excluded_students": excluded_students}
