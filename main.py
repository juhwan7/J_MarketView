import hashlib
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import requests
from bs4 import BeautifulSoup
import asyncio
from telegram import Bot
from telegram.constants import ParseMode
import os
import html
import google.generativeai as genai
import pdfplumber
from io import BytesIO
import datetime 
from collections import defaultdict 
import difflib

# ==============================================================================
# 1. 설정 (GitHub Secrets 활용)
# ==============================================================================
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

bot = Bot(token=TELEGRAM_TOKEN)
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-3.1-flash-lite-preview')
RECORD_FILE = 'sent_urls.txt' 
TITLE_RECORD_FILE = 'sent_titles.txt'

# ==============================================================================
# 2. 데이터 수집 함수
# ==============================================================================
def extract_text_from_pdf(pdf_url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(pdf_url, headers=headers, timeout=15)
        with pdfplumber.open(BytesIO(response.content)) as pdf:
            text = ""
            for page in pdf.pages[:3]: 
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        return text.strip()
    except Exception as e:
        return ""

def get_reports_by_category(category_name, url_path):
    headers = {'User-Agent': 'Mozilla/5.0'}
    reports_data = []
    
    today = datetime.datetime.now()
    # 💡 최대 2일 전 자정(00시 00분)으로 기준점 설정
    two_days_ago = (today - datetime.timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    for page in range(1, 10): 
        url = f"https://finance.naver.com/research/{url_path}?page={page}"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.encoding = 'euc-kr'
            soup = BeautifulSoup(response.text, 'html.parser')
            report_rows = soup.select('table.type_1 tr')
            
            if not report_rows or len(report_rows) <= 1:
                break
                
            for row in report_rows:
                tds = row.select('td')
                if len(tds) >= 5:
                    if len(tds) >= 6: 
                        t_idx, b_idx, p_idx, d_idx = 1, 2, 3, 4
                    else: 
                        t_idx, b_idx, p_idx, d_idx = 0, 1, 2, 3
                        
                    date_str = tds[d_idx].text.strip()
                    try:
                        report_date = datetime.datetime.strptime(date_str, '%y.%m.%d')
                        # 2일 전보다 과거 데이터면 수집 즉시 중단
                        if report_date < two_days_ago:
                            return reports_data
                    except:
                        continue
                        
                    title_tag = tds[t_idx].select_one('a')
                    broker = tds[b_idx].text.strip()
                    pdf_tag = tds[p_idx].select_one('a')
                    
                    if title_tag:
                        title = title_tag.text.strip()
                        link = f"https://finance.naver.com/research/{title_tag.get('href')}"
                        pdf_link = pdf_tag.get('href') if pdf_tag else ""
                        reports_data.append({
                            'category': category_name, 'title': title, 'broker': broker,
                            'date': date_str, 'link': link, 'pdf': pdf_link
                        })
        except Exception as e:
            pass
            
    return reports_data

async def fetch_content(report):
    content = ""
    if report['pdf']:
        content = extract_text_from_pdf(report['pdf'])
    if not content:
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(report['link'], headers=headers, timeout=10)
        res.encoding = 'euc-kr'
        soup = BeautifulSoup(res.text, 'html.parser')
        view_cnt = soup.select_one('.view_cnt')
        content = view_cnt.text.strip() if view_cnt else ""
    return content

# ==============================================================================
# 3. AI 분석 함수 
# ==============================================================================
async def analyze_daily_category_reports(target_date, category_name, report_list):
    try:
        combined_text = ""
        for rep in report_list:
            content = await fetch_content(rep)
            content_snippet = content[:2500] 
            combined_text += f"\n\n[제목: {rep['title']} (증권사: {rep['broker']})]\n내용: {content_snippet}"

        # 카테고리명에서 이모지 분리
        cat_emoji = category_name[0] if category_name else ""
        cat_text = category_name[1:].strip() if len(category_name) > 1 else category_name

        system_prompt = (
            f"당신은 상위 1% 주식 투자자를 위한 전문 애널리스트입니다. "
            f"다음은 '{target_date}'에 수집된 [{cat_text}] 섹터의 신규 리포트 모음입니다. "
            f"여러 증권사에서 동일한 기업이나 유사한 주제를 다룬 경우 '하나의 주제'로 묶어서 요약하세요. "
            f"단, 완전히 다른 주제나 섹터의 리포트들은 억지로 묶지 말고, 개별적인 분석 블록으로 완전히 분리해서 작성하세요."
        )

        prompt = f"""
        {system_prompt}
        
        [🚨 지침]
        1. 본문에 없는 내용은 지어내지 마세요.
        2. '간단명료하지만 깊이 있게' 작성하세요. (목표가, 영업이익률 등 수치 데이터 필수)
        3. 💡 중복 처리: 여러 리포트가 비슷한 내용(예: 특정 종목 호실적)을 말한다면, 한 블록 안에 증권사들을 종합하여 서술하세요.
        4. 💡 분리 처리: 서로 완전히 다른 주제의 리포트라면, 아래 [출력 형식] 블록 자체를 여러 개 생성하여 각각 분리해 주세요.
        
        [오늘 수집된 {cat_text} 본문 모음] 
        {combined_text}
        
        [출력 형식 가이드라인] (HTML <b> 태그 활용, 리포트 주제가 여러 개라면 아래 블록 전체를 주제별로 반복 출력할 것)
        
        {cat_emoji} <b>[{cat_text}] {target_date} (해당 리포트의 핵심 주제나 종목명) 분석</b>
        ━━━━━━━━━━━━━━━━━━━━
        
        <b>📝 [핵심 요약]</b>
        (해당 주제를 관통하는 가장 중요한 메시지 2~3줄)
        
        <b>🔍 [리포트 상세 분석]</b>
        - <b>발행처</b> : (관련 증권사명 모두 기재)
        - <b>핵심 논리</b> : (왜 좋게/나쁘게 보는지 구체적 이유 종합)
        - <b>중요 수치/전망</b> : (목표가 등 핵심 데이터)
        
        <b>💡 [실전 투자 적용 포인트]</b>
        (해당 주제에 대한 단기/스윙 투자 스탠스 조언)
        
        <br><br> (주제가 다를 경우 위 양식을 반복)
        """
        
        response = await asyncio.to_thread(model.generate_content, prompt)
        ai_text = response.text.replace('*', '•') 
        
        # HTML 태그 이스케이프 방지 처리
        ai_text = ai_text.replace('<b>', '[[B]]').replace('</b>', '[[/B]]') 
        ai_text = html.escape(ai_text) 
        ai_text = ai_text.replace('[[B]]', '<b>').replace('[[/B]]', '</b>')
        
        message = (
            f"{ai_text}\n\n"
            f"<i>💡 총 {len(report_list)}개의 신규 리포트가 처리되었습니다.</i>"
        )
        return message

    except Exception as e:
        if "429" in str(e) or "ResourceExhausted" in str(e):
            return "QUOTA_EXCEEDED"
        return f"❌ AI 분석 에러: {e}"

# ==============================================================================
# 4. 메인 실행 루틴
# ==============================================================================
# 💡 유사도 기준을 0.7에서 0.6으로 하향 조정
def is_similar_title(new_date, new_title, sent_titles_list, threshold=0.6):
    """새로운 리포트가 '같은 날짜'에 발송된 리포트와 유사한지 판별"""
    for old_item in sent_titles_list:
        try:
            old_date, old_title = old_item.split('|', 1)
        except ValueError:
            continue
            
        # 날짜가 다르면 무조건 다른 리포트로 취급
        if new_date != old_date:
            continue
            
        # 날짜가 같을 때만 제목 유사도 검사 진행
        similarity = difflib.SequenceMatcher(None, new_title, old_title).ratio()
        if similarity > threshold:
            return True
    return False

async def main():
    categories = [
        ('🌍 경제분석', 'economy_list.naver'),
        ('🌊 산업분석', 'industry_list.naver'),
        ('🎯 종목분석', 'company_list.naver'),
        ('📈 시황정보', 'market_info_list.naver'),
        ('💡 투자전략', 'invest_list.naver')
    ]
    
    print("🚀 [J_MarketView PRO] 신규 리포트 수집 및 요약을 시작합니다.")

    if not os.path.exists(RECORD_FILE):
        open(RECORD_FILE, 'w', encoding='utf-8').close()
    if not os.path.exists(TITLE_RECORD_FILE):
        open(TITLE_RECORD_FILE, 'w', encoding='utf-8').close()
    
    with open(RECORD_FILE, 'r', encoding='utf-8') as f:
        sent_urls = set(line.strip() for line in f if line.strip())
    with open(TITLE_RECORD_FILE, 'r', encoding='utf-8') as f:
        sent_titles = set(line.strip() for line in f if line.strip())

    all_reports = []
    for cat_name, url_path in categories:
        print(f"🔍 {cat_name} 최근 리포트 탐색 중...")
        reports = get_reports_by_category(cat_name, url_path)
        all_reports.extend(reports)
        
    new_reports = []
    for rep in all_reports:
        if rep['link'] in sent_urls:
            continue
        
        # 💡 날짜를 포함하여 유사도 검사 진행
        if is_similar_title(rep['date'], rep['title'], sent_titles):
            print(f"🔄 [같은 날짜 중복 제외] {rep['date']} - {rep['title']}")
            sent_urls.add(rep['link']) 
            continue
            
        new_reports.append(rep)

    if not new_reports:
        print("📭 새로 추가된 리포트가 없습니다. 실행을 종료합니다.")
        return

    reports_by_date_and_cat = defaultdict(lambda: defaultdict(list))
    for rep in new_reports:
        reports_by_date_and_cat[rep['date']][rep['category']].append(rep)
        
    sorted_dates = sorted(reports_by_date_and_cat.keys())
    
    for target_date in sorted_dates:
        for category_name, daily_cat_reports in reports_by_date_and_cat[target_date].items():
            print(f"🔥 [{target_date} - {category_name}] 신규 리포트 {len(daily_cat_reports)}건 심층 요약 시작!")
            
            message = await analyze_daily_category_reports(target_date, category_name, daily_cat_reports)
            
            if message == "QUOTA_EXCEEDED":
                print("😴 [AI API 한도 초과] 실행을 종료합니다. 다음 스케줄에 이어서 진행됩니다.")
                return 
            elif message.startswith("❌"):
                print(message)
                continue

            try:
                await bot.send_message(
                    chat_id=CHAT_ID, text=message, 
                    parse_mode=ParseMode.HTML, disable_web_page_preview=True
                )
                
                # 💡 성공적으로 전송 후 기록할 때 '날짜|제목' 포맷으로 저장
                with open(RECORD_FILE, 'a', encoding='utf-8') as f_url, \
                     open(TITLE_RECORD_FILE, 'a', encoding='utf-8') as f_title:
                    for rep in daily_cat_reports:
                        f_url.write(f"{rep['link']}\n")
                        
                        date_title_str = f"{rep['date']}|{rep['title']}"
                        f_title.write(f"{date_title_str}\n")
                        
                        sent_urls.add(rep['link'])
                        sent_titles.add(date_title_str)
                
                print(f"✅ [{target_date}] {category_name} 브리핑 전송 완료!")
                await asyncio.sleep(5) 
                
            except Exception as e:
                print(f"❌ 전송 실패: {e}")

    print("✅ 신규 리포트 처리 및 발송을 모두 완료했습니다.")

if __name__ == "__main__":
    asyncio.run(main())