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

# ==============================================================================
# 1. 설정 (GitHub Secrets 활용)
# ==============================================================================
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

bot = Bot(token=TELEGRAM_TOKEN)
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-3.1-flash-lite-preview')
RECORD_FILE = 'sent_urls.txt' # 개별 URL을 저장하여 새 리포트만 식별합니다.

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
    seven_days_ago = today - datetime.timedelta(days=7)
    
    for page in range(1, 10): # 탐색 페이지 수 10페이지로 최적화
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
                        if report_date < seven_days_ago:
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

        system_prompt = (
            f"당신은 상위 1% 주식 투자자를 위한 전문 애널리스트입니다. "
            f"다음은 '{target_date}'에 신규 발행된 [{category_name}] 섹터의 리포트 모음입니다. "
            f"내용을 단순 나열하지 말고, 전체 흐름을 관통하는 핵심을 짚어주되 증권사들이 제시한 '구체적인 근거, 수치, 심화 논리'를 반드시 포함하여 작성하세요."
        )

        prompt = f"""
        {system_prompt}
        
        [🚨 지침]
        1. 본문에 없는 내용은 절대 지어내지 마세요.
        2. '간단명료하지만 깊이 있게' 작성하세요. (초보자도 읽기 쉽지만, 전문적인 인사이트가 담겨야 함)
        3. 각 리포트에서 주장하는 핵심 근거(왜 오를 것인가/내릴 것인가)와 중요 목표가, 수치 데이터를 꼭 살려주세요.
        
        [오늘 수집된 {category_name} 본문 모음] 
        {combined_text}
        
        [출력 형식 가이드라인] (HTML <b> 태그 활용)
        
        <b>📊 [{target_date}] {category_name} 신규 리포트 브리핑</b>
        
        <b>📝 [섹터 핵심 요약]</b>
        (이 섹터 리포트들을 관통하는 가장 중요한 메시지 2~3줄)
        
        <b>🔍 [주요 리포트 심층 분석]</b>
        (가장 중요하거나 눈에 띄는 리포트 2~4개를 선정하여 아래 양식으로 각각 깊이 있게 분석)
        - <b>(종목명 또는 주제)</b> : (증권사명)
          • 핵심 논리 : (왜 좋게/나쁘게 보는지 구체적 이유)
          • 중요 수치/전망 : (목표가, 영업이익률 변화, 핵심 데이터 등)
          
        <b>💡 [실전 투자 적용 포인트]</b>
        (이 섹터의 내용을 바탕으로 단기/스윙 투자자가 어떤 스탠스를 취해야 하는지 현실적인 조언)
        """
        
        response = await asyncio.to_thread(model.generate_content, prompt)
        ai_text = response.text.replace('*', '•') 
        
        ai_text = ai_text.replace('<b>', '[[B]]').replace('</b>', '[[/B]]') 
        ai_text = html.escape(ai_text) 
        ai_text = ai_text.replace('[[B]]', '<b>').replace('[[/B]]', '</b>')
        
        message = (
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>📚 [J_MarketView] {category_name} 업데이트 분석</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{ai_text}\n\n"
            f"<i>💡 총 {len(report_list)}개의 신규 리포트가 요약되었습니다.</i>\n"
            f"━━━━━━━━━━━━━━━━━━━━"
        )
        return message

    except Exception as e:
        if "429" in str(e) or "ResourceExhausted" in str(e):
            return "QUOTA_EXCEEDED"
        return f"❌ AI 분석 에러: {e}"

# ==============================================================================
# 4. 메인 실행 루틴
# ==============================================================================
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
    
    # 이미 전송한 리포트 URL 불러오기
    with open(RECORD_FILE, 'r', encoding='utf-8') as f:
        sent_urls = set(line.strip() for line in f if line.strip())

    all_reports = []
    for cat_name, url_path in categories:
        print(f"🔍 {cat_name} 최근 리포트 탐색 중...")
        reports = get_reports_by_category(cat_name, url_path)
        all_reports.extend(reports)
        
    # 새로운 리포트만 필터링
    new_reports = [rep for rep in all_reports if rep['link'] not in sent_urls]

    if not new_reports:
        print("📭 새로 추가된 리포트가 없습니다. 실행을 종료합니다.")
        return

    # 날짜별 -> 섹터별로 그룹화
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
                
                # 성공적으로 전송한 리포트들의 URL을 기록에 추가
                with open(RECORD_FILE, 'a', encoding='utf-8') as f:
                    for rep in daily_cat_reports:
                        f.write(f"{rep['link']}\n")
                        sent_urls.add(rep['link'])
                
                print(f"✅ [{target_date}] {category_name} 브리핑 전송 완료!")
                await asyncio.sleep(20) # 도배 방지용 약간의 대기
                
            except Exception as e:
                print(f"❌ 전송 실패: {e}")

    print("✅ 신규 리포트 처리 및 발송을 모두 완료했습니다.")

if __name__ == "__main__":
    asyncio.run(main())