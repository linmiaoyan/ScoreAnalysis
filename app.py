"""
成绩分析系统 - 独立Flask应用
"""
import os
import logging
import pandas as pd
from flask import Flask, render_template, request, jsonify
from werkzeug.utils import secure_filename
from datetime import datetime

from data_processor import (read_school_data, read_league_data, analyze_school_scores, 
                            analyze_league_scores, analyze_school_total_score, 
                            analyze_school_subjects_by_class, analyze_subject_score_lines,
                            analyze_class_subjects_table, calculate_class_assessment,
                            build_school_data_from_league)

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static', static_url_path='/static')
app.config['SECRET_KEY'] = 'score_analysis_secret_key'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB

# 配置上传文件夹
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads')
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

# 确保上传文件夹存在
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)


def _get_school_data_from_sources(school_path: str,
                                  league_path: str,
                                  school_name: str,
                                  school_alias: str):
    """
    根据传入的路径和学校名称，优先使用 school_path 读取我校数据；
    如果没有上传我校文件，则尝试从联盟总成绩文件中按“学校”字段筛选构造我校数据。
    """
    school_name = school_name or ''
    school_alias = school_alias or ''
    match_name = school_alias.strip() if school_alias.strip() else school_name.strip()

    # 1. 优先使用单独上传的我校文件
    if school_path:
        school_data = read_school_data(school_path)
        logger.info(
            f"_get_school_data_from_sources: 使用独立我校文件读取成功，"
            f"学科数量: {len(school_data)}, 学科列表: {list(school_data.keys())}"
        )
        return school_data

    # 2. 没有我校文件时，尝试从联盟总成绩中构建
    if league_path:
        if not match_name:
            raise ValueError("未提供学校名称或别名，无法从联盟总成绩中筛选我校数据")

        league_df = read_league_data(league_path)
        derived_school_data = build_school_data_from_league(league_df, match_name)

        if not derived_school_data:
            raise ValueError(f"在联盟总成绩中未找到学校名称为“{match_name}”的任何记录，无法构建我校数据")

        logger.info(
            f"_get_school_data_from_sources: 未上传我校文件，已从联盟总成绩中构建我校数据，"
            f"学科数量: {len(derived_school_data)}, 学科列表: {list(derived_school_data.keys())}"
        )
        return derived_school_data

    # 3. 两种来源都没有
    raise ValueError("没有上传我校文件或联盟总成绩文件，无法获取我校数据")

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/')
def index():
    """主页"""
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_files():
    """上传文件"""
    try:
        school_file = request.files.get('school_file')
        league_file = request.files.get('league_file')
        
        school_path = None
        league_path = None
        
        # 允许只上传一个文件
        if school_file and school_file.filename:
            school_filename = secure_filename(school_file.filename)
            school_path = os.path.join(UPLOAD_FOLDER, f"school_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{school_filename}")
            school_file.save(school_path)
        
        if league_file and league_file.filename:
            league_filename = secure_filename(league_file.filename)
            league_path = os.path.join(UPLOAD_FOLDER, f"league_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{league_filename}")
            league_file.save(league_path)
        
        if not school_path and not league_path:
            return jsonify({'success': False, 'message': '请至少上传一个文件'}), 400
        
        # 读取数据
        result = {
            'success': True,
            'message': '文件上传成功',
            'school_path': school_path,
            'league_path': league_path
        }
        
        try:
            # 上传时只读取基本信息，不读取全部数据（避免重复读取）
            if school_path:
                # 只读取学科列表，不读取全部数据
                # 优先使用 read_school_data 的逻辑（同时兼容"多学科标签页"和"单表CDE+学科列"两种格式）
                subjects = []
                try:
                    school_data_preview = read_school_data(school_path)
                    subjects = list(school_data_preview.keys())
                    logger.info(f"上传验证：通过 read_school_data 检测到 {len(subjects)} 个学科: {subjects}")
                except Exception as e:
                    # 如果使用 read_school_data 失败，则退回到旧逻辑：按标签页名称推断学科
                    logger.warning(f"通过 read_school_data 检测学科失败，改用标签页名称推断：{str(e)}")
                    excel_file = pd.ExcelFile(school_path)
                    sheet_names = excel_file.sheet_names
                    # 旧逻辑：排除“总分”标签页，其余视为学科
                    subjects = [name for name in sheet_names if name != '总分']
                    logger.info(f"上传验证（标签页方式）：找到 {len(subjects)} 个学科: {subjects}")
                
                result['subjects'] = subjects
                # 不读取全部数据，只返回学科列表
                # 详细日志在上面的逻辑里已输出
            
            if league_path:
                # 只读取基本信息，不读取全部数据
                df = pd.read_excel(league_path, sheet_name='分数', nrows=0)  # 只读表头
                result['league_columns'] = list(df.columns) if not df.empty else []
            
            return jsonify(result)
        except Exception as e:
            logger.error(f"验证文件失败: {str(e)}", exc_info=True)
            return jsonify({'success': False, 'message': f'文件验证失败: {str(e)}'}), 500
        
    except Exception as e:
        logger.error(f"上传文件失败: {str(e)}", exc_info=True)
        return jsonify({'success': False, 'message': f'上传失败: {str(e)}'}), 500


@app.route('/analyze', methods=['POST'])
def analyze():
    """执行分析"""
    try:
        data = request.get_json()
        school_path = data.get('school_path')
        league_path = data.get('league_path')
        score_lines = data.get('score_lines', [])
        school_name = data.get('school_name', '')
        school_alias = data.get('school_alias', '')
        
        if not school_path and not league_path:
            return jsonify({'success': False, 'message': '请先上传文件'}), 400
        
        if not score_lines:
            return jsonify({'success': False, 'message': '请输入至少一条分数线'}), 400
        
        # 转换分数线为浮点数
        try:
            score_lines = [float(line) for line in score_lines]
        except:
            return jsonify({'success': False, 'message': '分数线格式错误'}), 400
        
        school_analysis = {}
        league_analysis = {}
        league_data = None
        
        # 如上传了联盟总成绩文件，先读取一次联盟数据，后续复用
        if league_path:
            league_data = read_league_data(league_path)
        
        # 读取并分析我校数据（允许来自“我校文件”或“联盟总成绩”）
        try:
            school_data = _get_school_data_from_sources(
                school_path=school_path,
                league_path=league_path,
                school_name=school_name,
                school_alias=school_alias
            )
            
            logger.info(
                f"分析接口：成功获取我校数据，学科数量: {len(school_data)}, "
                f"学科列表: {list(school_data.keys())}"
            )
            
            # 打印每个学科的详细信息
            for subject, df in school_data.items():
                if not df.empty and '得分' in df.columns:
                    avg = df['得分'].mean()
                    count = len(df)
                    logger.info(f"  学科 {subject}: {count}人, 平均分={avg:.2f}")
            
            school_analysis = analyze_school_scores(school_data, score_lines)
            logger.info(
                f"我校数据分析完成，学科数量: {len(school_analysis)}, "
                f"学科列表: {list(school_analysis.keys())}"
            )
        except ValueError as ve:
            # 如果只是无法获取我校数据，则不终止整个接口，只在返回中不给 school_analysis
            logger.warning(f"分析接口：获取我校数据失败：{str(ve)}")
        
        # 读取并分析联盟数据
        if league_data is not None:
            match_name = school_alias.strip() if school_alias.strip() else school_name.strip()
            league_analysis = analyze_league_scores(league_data, match_name, score_lines, school_name)
        
        result = {
            'success': True,
            'school_analysis': school_analysis,
            'league_analysis': league_analysis
        }
        
        # 返回文件路径，用于后续查询详细数据
        if school_path:
            result['school_path'] = school_path
        if league_path:
            result['league_path'] = league_path
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"分析失败: {str(e)}", exc_info=True)
        return jsonify({'success': False, 'message': f'分析失败: {str(e)}'}), 500


@app.route('/preview', methods=['POST'])
def preview_data():
    """预览数据"""
    try:
        data = request.get_json()
        file_type = data.get('file_type')  # 'school' or 'league'
        file_path = data.get('file_path')
        
        if not file_path:
            return jsonify({'success': False, 'message': '文件路径不存在'}), 400
        
        if file_type == 'school':
            school_data = read_school_data(file_path)
            preview = {}
            for subject, df in school_data.items():
                preview[subject] = {
                    'columns': list(df.columns),
                    'row_count': len(df),
                    'sample_data': df.head(10).to_dict('records')
                }
            return jsonify({'success': True, 'preview': preview})
        
        elif file_type == 'league':
            league_data = read_league_data(file_path)
            return jsonify({
                'success': True,
                'preview': {
                    'columns': list(league_data.columns),
                    'row_count': len(league_data),
                    'sample_data': league_data.head(10).to_dict('records'),
                    'schools': list(league_data['学校'].unique()) if '学校' in league_data.columns else []
                }
            })
        
        return jsonify({'success': False, 'message': '无效的文件类型'}), 400
        
    except Exception as e:
        logger.error(f"预览数据失败: {str(e)}")
        return jsonify({'success': False, 'message': f'预览失败: {str(e)}'}), 500


@app.route('/analyze_school_subjects', methods=['POST'])
def analyze_school_subjects():
    """分析我校各学科成绩，按班级对比"""
    try:
        data = request.get_json()
        school_path = data.get('school_path')
        league_path = data.get('league_path')
        school_name = data.get('school_name', '')
        school_alias = data.get('school_alias', '')
        score_lines = data.get('score_lines', [])  # 保留参数以兼容，但不使用
        
        if not school_path and not league_path:
            return jsonify({'success': False, 'message': '请先上传我校数据文件或联盟总成绩文件'}), 400
        
        # 读取我校数据（允许来源于我校文件或联盟总成绩）
        try:
            school_data = _get_school_data_from_sources(
                school_path=school_path,
                league_path=league_path,
                school_name=school_name,
                school_alias=school_alias
            )
        except ValueError as ve:
            logger.warning(f"分析我校各学科时获取我校数据失败: {str(ve)}")
            return jsonify({'success': False, 'message': str(ve)}), 400
        
        logger.info(f"读取我校数据，学科数量: {len(school_data)}, 学科列表: {list(school_data.keys())}")
        
        # 分析各学科按班级对比（不需要分数线）
        analysis_result = analyze_school_subjects_by_class(school_data)
        logger.info(f"我校各学科分析完成，学科数量: {len(analysis_result)}")
        
        result = {
            'success': True,
            'analysis': analysis_result
        }
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"分析失败: {str(e)}", exc_info=True)
        return jsonify({'success': False, 'message': f'分析失败: {str(e)}'}), 500


@app.route('/analyze_school_total', methods=['POST'])
def analyze_school_total():
    """分析我校成绩的总分和分数线情况"""
    try:
        data = request.get_json()
        school_path = data.get('school_path')
        league_path = data.get('league_path')
        school_name = data.get('school_name', '')
        school_alias = data.get('school_alias', '')
        score_lines = data.get('score_lines', [])
        
        if not school_path and not league_path:
            return jsonify({'success': False, 'message': '请先上传我校数据文件或联盟总成绩文件'}), 400
        
        if not score_lines:
            return jsonify({'success': False, 'message': '请输入至少一条分数线'}), 400
        
        # 转换分数线为浮点数
        try:
            score_lines = [float(line) for line in score_lines]
        except:
            return jsonify({'success': False, 'message': '分数线格式错误'}), 400
        
        # 读取我校数据
        try:
            school_data = _get_school_data_from_sources(
                school_path=school_path,
                league_path=league_path,
                school_name=school_name,
                school_alias=school_alias
            )
        except ValueError as ve:
            logger.warning(f"分析我校总分时获取我校数据失败: {str(ve)}")
            return jsonify({'success': False, 'message': str(ve)}), 400
        
        logger.info(f"读取我校数据，学科数量: {len(school_data)}, 学科列表: {list(school_data.keys())}")
        
        # 分析总分和分数线
        analysis_result = analyze_school_total_score(school_data, score_lines)
        logger.info(f"我校总分分析完成，分数线数量: {len(analysis_result)}")
        
        result = {
            'success': True,
            'analysis': analysis_result
        }
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"分析失败: {str(e)}", exc_info=True)
        return jsonify({'success': False, 'message': f'分析失败: {str(e)}'}), 500


@app.route('/class_detail', methods=['POST'])
def get_class_detail():
    """获取班级详细成绩"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': '请求数据为空'}), 400
            
        file_path = data.get('file_path')
        subject = data.get('subject')
        class_name = data.get('class_name')
        
        if not file_path or not subject or not class_name:
            return jsonify({'success': False, 'message': '参数不完整'}), 400
        
        # 检查文件是否存在
        if not os.path.exists(file_path):
            logger.warning(f"文件不存在: {file_path}")
            return jsonify({'success': False, 'message': '文件不存在'}), 404
        
        # 读取数据
        school_data = read_school_data(file_path)
        
        if subject not in school_data:
            return jsonify({'success': False, 'message': f'未找到学科: {subject}'}), 400
        
        df = school_data[subject]
        
        # 筛选指定班级
        if '班级' in df.columns:
            class_df = df[df['班级'].astype(str) == str(class_name)].copy()
        else:
            return jsonify({'success': False, 'message': '数据中没有班级列'}), 400
        
        if len(class_df) == 0:
            return jsonify({
                'success': True,
                'subject': subject,
                'class_name': class_name,
                'total_count': 0,
                'students': []
            })
        
        # 按得分排序（降序）
        if '得分' in class_df.columns:
            class_df = class_df.sort_values('得分', ascending=False)
        
        # 转换为字典列表，并处理NaN值
        students = []
        for _, row in class_df.iterrows():
            student_dict = {}
            for col in class_df.columns:
                value = row[col]
                # 将NaN、NaT等转换为None（JSON中为null）
                if pd.isna(value):
                    student_dict[col] = None
                else:
                    # 如果是数值类型，转换为Python原生类型
                    if pd.api.types.is_numeric_dtype(type(value)):
                        student_dict[col] = float(value) if not pd.isna(value) else None
                    else:
                        student_dict[col] = str(value) if value is not None else None
            students.append(student_dict)
        
        return jsonify({
            'success': True,
            'subject': subject,
            'class_name': class_name,
            'total_count': len(students),
            'students': students
        })
        
    except Exception as e:
        logger.error(f"获取班级详细成绩失败: {str(e)}", exc_info=True)
        return jsonify({'success': False, 'message': f'获取失败: {str(e)}'}), 500


@app.route('/analyze_subject_lines', methods=['POST'])
def analyze_subject_lines():
    """分析各学科的分数线情况"""
    try:
        data = request.get_json()
        school_path = data.get('school_path')
        league_path = data.get('league_path')
        school_name = data.get('school_name', '')
        school_alias = data.get('school_alias', '')
        total_score_line = data.get('total_score_line')
        subject_score_lines = data.get('subject_score_lines', {})
        
        if not school_path and not league_path:
            return jsonify({'success': False, 'message': '请先上传我校数据文件或联盟总成绩文件'}), 400
        
        if not total_score_line:
            return jsonify({'success': False, 'message': '请输入总分分数线'}), 400
        
        # 读取数据
        try:
            school_data = _get_school_data_from_sources(
                school_path=school_path,
                league_path=league_path,
                school_name=school_name,
                school_alias=school_alias
            )
        except ValueError as ve:
            logger.warning(f"分析学科分数线时获取我校数据失败: {str(ve)}")
            return jsonify({'success': False, 'message': str(ve)}), 400
        
        # 分析学科分数线
        result = analyze_subject_score_lines(school_data, float(total_score_line), subject_score_lines)
        
        return jsonify({'success': True, 'analysis': result})
        
    except Exception as e:
        logger.error(f"分析学科分数线失败: {str(e)}", exc_info=True)
        return jsonify({'success': False, 'message': f'分析失败: {str(e)}'}), 500


@app.route('/analyze_class_subjects', methods=['POST'])
def analyze_class_subjects():
    """分析班级各科情况表格"""
    try:
        data = request.get_json()
        school_path = data.get('school_path')
        league_path = data.get('league_path')
        school_name = data.get('school_name', '')
        school_alias = data.get('school_alias', '')
        score_line = data.get('score_line')
        subject_score_lines = data.get('subject_score_lines', {})
        
        if not school_path and not league_path:
            return jsonify({'success': False, 'message': '请先上传我校数据文件或联盟总成绩文件'}), 400
        
        if not score_line:
            return jsonify({'success': False, 'message': '请输入分数线'}), 400
        
        # 读取数据
        try:
            school_data = _get_school_data_from_sources(
                school_path=school_path,
                league_path=league_path,
                school_name=school_name,
                school_alias=school_alias
            )
        except ValueError as ve:
            logger.warning(f"分析班级各科情况时获取我校数据失败: {str(ve)}")
            return jsonify({'success': False, 'message': str(ve)}), 400
        
        # 分析班级各科情况
        result = analyze_class_subjects_table(school_data, float(score_line), subject_score_lines)
        
        return jsonify({'success': True, 'analysis': result})
        
    except Exception as e:
        logger.error(f"分析班级各科情况失败: {str(e)}", exc_info=True)
        return jsonify({'success': False, 'message': f'分析失败: {str(e)}'}), 500


@app.route('/calculate_class_assessment', methods=['POST'])
def calculate_class_assessment_endpoint():
    """计算班级考核分"""
    try:
        data = request.get_json()
        school_path = data.get('school_path')
        league_path = data.get('league_path')
        school_name = data.get('school_name', '')
        school_alias = data.get('school_alias', '')
        tekong_line = data.get('tekong_line')
        yiduan_line = data.get('yiduan_line')
        
        if not school_path and not league_path:
            return jsonify({'success': False, 'message': '请先上传我校数据文件或联盟总成绩文件'}), 400
        
        if not tekong_line or not yiduan_line:
            return jsonify({'success': False, 'message': '请输入特控线和一段线'}), 400
        
        # 读取数据
        try:
            school_data = _get_school_data_from_sources(
                school_path=school_path,
                league_path=league_path,
                school_name=school_name,
                school_alias=school_alias
            )
        except ValueError as ve:
            logger.warning(f"计算班级考核分时获取我校数据失败: {str(ve)}")
            return jsonify({'success': False, 'message': str(ve)}), 400
        
        # 计算班级考核分
        result = calculate_class_assessment(school_data, float(tekong_line), float(yiduan_line))
        
        return jsonify({'success': True, 'results': result})
        
    except Exception as e:
        logger.error(f"计算班级考核分失败: {str(e)}", exc_info=True)
        return jsonify({'success': False, 'message': f'计算失败: {str(e)}'}), 500


@app.route('/export_excel', methods=['POST'])
def export_excel():
    """导出分析结果为Excel文件"""
    try:
        from flask import send_file
        from io import BytesIO
        
        data = request.get_json()
        export_data = data.get('export_data', {})
        
        # 创建Excel工作簿
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # 导出各类分析结果
            sheet_idx = 0
            
            # 1. 班级考核结果
            if 'class_assessment' in export_data:
                assessment_data = export_data['class_assessment']
                df_assessment = pd.DataFrame(assessment_data)
                if not df_assessment.empty:
                    # 列名改为中文
                    df_assessment = df_assessment.rename(columns={
                        'rank': '排名',
                        'class_name': '班级',
                        'total_students': '总人数',
                        'tekong_passed': '特控过线人数',
                        'tekong_rate': '特控率(%)',
                        'yiduan_passed': '一段过线人数',
                        'yiduan_rate': '一段率(%)',
                        'assessment_score': '考核分'
                    })
                df_assessment.to_excel(writer, sheet_name='班级考核结果', index=False)
                sheet_idx += 1
            
            # 1.1 班级考核中被剔除的学生
            if 'class_assessment_excluded' in export_data:
                excluded_data = export_data['class_assessment_excluded']
                df_excluded = pd.DataFrame(excluded_data)
                if not df_excluded.empty:
                    df_excluded = df_excluded.rename(columns={
                        '班级': '班级',
                        '姓名': '姓名',
                        '原因': '原因'
                    })
                df_excluded.to_excel(writer, sheet_name='班级考核-未纳入学生', index=False)
            
            # 2. 班级各科情况（特控线）
            if 'class_subjects_tekong' in export_data:
                class_subjects = export_data['class_subjects_tekong']
                # 构建表格数据
                all_classes = sorted(class_subjects.get('classes', {}).keys())
                all_subjects = list(class_subjects.get('subject_lines', {}).keys())
                
                # 创建同时包含过线人数和过线率的表
                passed_data = []
                for class_name in all_classes:
                    row = {'班级': class_name}
                    for subject in all_subjects:
                        if subject in class_subjects['classes'].get(class_name, {}):
                            info = class_subjects['classes'][class_name][subject]
                            row[subject + '_过线人数'] = info.get('passed_count', 0)
                            row[subject + '_过线率(%)'] = info.get('pass_rate', 0)
                        else:
                            row[subject + '_过线人数'] = 0
                            row[subject + '_过线率(%)'] = 0
                    passed_data.append(row)
                
                df_passed = pd.DataFrame(passed_data)
                df_passed.to_excel(writer, sheet_name='特控线-班级各科过线情况', index=False)
                
                # 添加图表（使用openpyxl）
                try:
                    from openpyxl.chart import BarChart, LineChart, Reference, Series
                    from openpyxl.chart.axis import DateAxis
                    
                    workbook = writer.book
                    worksheet = workbook['特控线-班级各科过线情况']
                    
                    # 创建组合图表（柱状图+折线图）
                    chart = BarChart()
                    chart.type = "col"
                    chart.style = 10
                    chart.title = "特控线-班级各科过线情况"
                    chart.y_axis.title = '过线人数'
                    chart.x_axis.title = '班级'
                    
                    # 添加过线人数数据（柱状图）
                    data_start_row = 2
                    data_end_row = len(all_classes) + 1
                    
                    for idx, subject in enumerate(all_subjects):
                        col_idx = 2 + idx * 2  # 过线人数列（跳过班级列，每学科占2列）
                        values = Reference(worksheet, min_col=col_idx, min_row=data_start_row, max_row=data_end_row)
                        series = Series(values, title=subject + '过线人数')
                        chart.series.append(series)
                    
                    # 设置图表位置
                    chart.width = 15
                    chart.height = 10
                    worksheet.add_chart(chart, "A" + str(data_end_row + 3))
                    
                    # 创建过线率折线图
                    line_chart = LineChart()
                    line_chart.title = "特控线-班级各科过线率"
                    line_chart.y_axis.title = '过线率(%)'
                    line_chart.x_axis.title = '班级'
                    
                    # 添加班级名称作为X轴
                    cats = Reference(worksheet, min_col=1, min_row=data_start_row, max_row=data_end_row)
                    line_chart.set_categories(cats)
                    
                    # 添加过线率数据
                    for idx, subject in enumerate(all_subjects):
                        col_idx = 3 + idx * 2  # 过线率列
                        values = Reference(worksheet, min_col=col_idx, min_row=data_start_row, max_row=data_end_row)
                        series = Series(values, title=subject + '过线率(%)')
                        line_chart.series.append(series)
                    
                    line_chart.width = 15
                    line_chart.height = 10
                    worksheet.add_chart(line_chart, "A" + str(data_end_row + 18))
                except Exception as e:
                    logger.warning(f"创建特控线图表失败: {str(e)}")
                
                sheet_idx += 1
            
            # 3. 班级各科情况（一段线）
            if 'class_subjects_yiduan' in export_data:
                class_subjects = export_data['class_subjects_yiduan']
                # 构建表格数据
                all_classes = sorted(class_subjects.get('classes', {}).keys())
                all_subjects = list(class_subjects.get('subject_lines', {}).keys())
                
                # 创建同时包含过线人数和过线率的表
                passed_data = []
                for class_name in all_classes:
                    row = {'班级': class_name}
                    for subject in all_subjects:
                        if subject in class_subjects['classes'].get(class_name, {}):
                            info = class_subjects['classes'][class_name][subject]
                            row[subject + '_过线人数'] = info.get('passed_count', 0)
                            row[subject + '_过线率(%)'] = info.get('pass_rate', 0)
                        else:
                            row[subject + '_过线人数'] = 0
                            row[subject + '_过线率(%)'] = 0
                    passed_data.append(row)
                
                df_passed = pd.DataFrame(passed_data)
                df_passed.to_excel(writer, sheet_name='一段线-班级各科过线情况', index=False)
                
                # 添加图表（使用openpyxl）
                try:
                    from openpyxl.chart import BarChart, LineChart, Reference, Series
                    from openpyxl.chart.axis import DateAxis
                    
                    workbook = writer.book
                    worksheet = workbook['一段线-班级各科过线情况']
                    
                    # 创建组合图表（柱状图+折线图）
                    chart = BarChart()
                    chart.type = "col"
                    chart.style = 10
                    chart.title = "一段线-班级各科过线情况"
                    chart.y_axis.title = '过线人数'
                    chart.x_axis.title = '班级'
                    
                    # 添加过线人数数据（柱状图）
                    data_start_row = 2
                    data_end_row = len(all_classes) + 1
                    
                    for idx, subject in enumerate(all_subjects):
                        col_idx = 2 + idx * 2  # 过线人数列
                        values = Reference(worksheet, min_col=col_idx, min_row=data_start_row, max_row=data_end_row)
                        series = Series(values, title=subject + '过线人数')
                        chart.series.append(series)
                    
                    # 设置图表位置
                    chart.width = 15
                    chart.height = 10
                    worksheet.add_chart(chart, "A" + str(data_end_row + 3))
                    
                    # 创建过线率折线图
                    line_chart = LineChart()
                    line_chart.title = "一段线-班级各科过线率"
                    line_chart.y_axis.title = '过线率(%)'
                    line_chart.x_axis.title = '班级'
                    
                    # 添加班级名称作为X轴
                    cats = Reference(worksheet, min_col=1, min_row=data_start_row, max_row=data_end_row)
                    line_chart.set_categories(cats)
                    
                    # 添加过线率数据
                    for idx, subject in enumerate(all_subjects):
                        col_idx = 3 + idx * 2  # 过线率列
                        values = Reference(worksheet, min_col=col_idx, min_row=data_start_row, max_row=data_end_row)
                        series = Series(values, title=subject + '过线率(%)')
                        line_chart.series.append(series)
                    
                    line_chart.width = 15
                    line_chart.height = 10
                    worksheet.add_chart(line_chart, "A" + str(data_end_row + 18))
                except Exception as e:
                    logger.warning(f"创建一段线图表失败: {str(e)}")
                
                sheet_idx += 1
            
            # 4. 学科分数线详情
            if 'subject_lines_tekong' in export_data:
                subject_lines = export_data['subject_lines_tekong']
                for subject, subject_data in subject_lines.get('subjects', {}).items():
                    df_subject = pd.DataFrame(subject_data.get('class_stats', []))
                    if not df_subject.empty:
                        df_subject = df_subject.rename(columns={
                            'class_name': '班级',
                            'total_students': '总人数',
                            'passed_count': '过线人数',
                            'pass_rate': '过线率(%)',
                            'average_score': '平均分'
                        })
                        df_subject.to_excel(writer, sheet_name=f'特控线-{subject}', index=False)
            
            if 'subject_lines_yiduan' in export_data:
                subject_lines = export_data['subject_lines_yiduan']
                for subject, subject_data in subject_lines.get('subjects', {}).items():
                    df_subject = pd.DataFrame(subject_data.get('class_stats', []))
                    if not df_subject.empty:
                        df_subject = df_subject.rename(columns={
                            'class_name': '班级',
                            'total_students': '总人数',
                            'passed_count': '过线人数',
                            'pass_rate': '过线率(%)',
                            'average_score': '平均分'
                        })
                        df_subject.to_excel(writer, sheet_name=f'一段线-{subject}', index=False)
        
        output.seek(0)
        
        # 生成文件名
        filename = f"成绩分析结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        logger.error(f"导出Excel失败: {str(e)}", exc_info=True)
        return jsonify({'success': False, 'message': f'导出失败: {str(e)}'}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5007)
