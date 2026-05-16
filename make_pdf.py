from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                 TableStyle, HRFlowable, KeepTogether)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
import os, subprocess

# ── 한글 폰트 찾기 ──────────────────────────────────────────────
def find_font(names):
    dirs = ['/usr/share/fonts', '/usr/local/share/fonts',
            os.path.expanduser('~/.fonts')]
    for name in names:
        for d in dirs:
            for root, _, files in os.walk(d):
                for f in files:
                    if name.lower() in f.lower() and f.endswith('.ttf'):
                        return os.path.join(root, f)
    return None

nanum = find_font(['NanumGothic.ttf', 'NanumGothicBold.ttf'])
if not nanum:
    subprocess.run(['apt-get', 'install', '-y', 'fonts-nanum'], capture_output=True)
    nanum = find_font(['NanumGothic.ttf'])

nanum_r = find_font(['NanumGothic.ttf'])
nanum_b = find_font(['NanumGothicBold.ttf', 'NanumGothicExtraBold.ttf'])

if nanum_r:
    pdfmetrics.registerFont(TTFont('NanumGothic', nanum_r))
if nanum_b:
    pdfmetrics.registerFont(TTFont('NanumGothicBold', nanum_b))

FONT = 'NanumGothic' if nanum_r else 'Helvetica'
FONT_B = 'NanumGothicBold' if nanum_b else 'Helvetica-Bold'

# ── 색상 ────────────────────────────────────────────────────────
RED   = colors.HexColor('#C00000')
NAVY  = colors.HexColor('#2E4057')
LGRAY = colors.HexColor('#F5F5F5')
WHITE = colors.white
GOLD  = colors.HexColor('#FFE0E0')
BLUE  = colors.HexColor('#E8F4FF')
GREEN = colors.HexColor('#E8FFE8')

# ── 스타일 ──────────────────────────────────────────────────────
styles = getSampleStyleSheet()

def S(name, font=FONT, size=10, color=colors.black, bold=False, align=TA_LEFT,
      leading=14, spaceBefore=2, spaceAfter=2):
    return ParagraphStyle(name, fontName=FONT_B if bold else font,
                          fontSize=size, textColor=color, alignment=align,
                          leading=leading, spaceBefore=spaceBefore, spaceAfter=spaceAfter)

s_title   = S('Title',  font=FONT_B, size=20, color=RED, align=TA_CENTER, leading=28, spaceBefore=0, spaceAfter=4)
s_sub     = S('Sub',    size=11, color=colors.HexColor('#606060'), align=TA_CENTER)
s_h1      = S('H1',     font=FONT_B, size=14, color=RED, leading=20, spaceBefore=12, spaceAfter=4)
s_h2      = S('H2',     font=FONT_B, size=11, color=NAVY, leading=16, spaceBefore=8, spaceAfter=3)
s_body    = S('Body',   size=10, leading=15, spaceBefore=1, spaceAfter=1)
s_bullet  = S('Bullet', size=10, leading=15, spaceBefore=1, spaceAfter=1)
s_box     = S('Box',    font=FONT_B, size=11, color=RED, align=TA_CENTER, leading=16)
s_cell    = S('Cell',   size=9, leading=13)
s_cellb   = S('CellB',  font=FONT_B, size=9, color=WHITE, align=TA_CENTER, leading=13)

def h1(text): return Paragraph(text, s_h1)
def h2(text): return Paragraph(text, s_h2)
def body(text): return Paragraph(text, s_body)
def bullet(text): return Paragraph(f'▸ {text}', s_bullet)
def sp(h=6): return Spacer(1, h)
def hr(): return HRFlowable(width='100%', thickness=0.5, color=colors.HexColor('#CCCCCC'))

def box(text, bg=GOLD, tc=RED):
    t = Table([[Paragraph(text, ParagraphStyle('bx', fontName=FONT_B, fontSize=11,
                textColor=tc, alignment=TA_CENTER, leading=16))]],
              colWidths=[15.5*cm])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), bg),
        ('ROWBACKGROUNDS', (0,0), (-1,-1), [bg]),
        ('BOX', (0,0), (-1,-1), 0.5, colors.HexColor('#BBBBBB')),
        ('TOPPADDING', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ('LEFTPADDING', (0,0), (-1,-1), 10),
        ('RIGHTPADDING', (0,0), (-1,-1), 10),
    ]))
    return t

def make_table(header, rows, col_widths=None, header_bg=NAVY):
    data = [[Paragraph(h, s_cellb) for h in header]]
    for row in rows:
        data.append([Paragraph(str(c), s_cell) for c in row])
    t = Table(data, colWidths=col_widths)
    style = [
        ('BACKGROUND', (0,0), (-1,0), header_bg),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [WHITE, LGRAY]),
        ('BOX', (0,0), (-1,-1), 0.5, colors.HexColor('#AAAAAA')),
        ('INNERGRID', (0,0), (-1,-1), 0.3, colors.HexColor('#CCCCCC')),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('LEFTPADDING', (0,0), (-1,-1), 5),
        ('RIGHTPADDING', (0,0), (-1,-1), 5),
    ]
    t.setStyle(TableStyle(style))
    return t

# ── 문서 구성 ───────────────────────────────────────────────────
elements = []
W = 15.5 * cm

# 표지
elements += [
    sp(20),
    Paragraph('2028 대입 개편 및 교육 전략 설명회', s_title),
    sp(6),
    Paragraph('2026년 5월 16일 | 구주이배 교육', s_sub),
    sp(4),
    hr(),
    sp(20),
]

# 1. 2028 수능대비의 핵심
elements += [h1('1. 2028 수능대비의 핵심'), h2('■ 개편(2028~) 수능 과목 및 평가 방식')]
elements.append(make_table(
    ['과목', '평가 방식'],
    [
        ['화법과 언어, 독서와 작문, 문학', '9등급 상대평가'],
        ['대수, 미적분Ⅰ, 확률과 통계', '9등급 상대평가'],
        ['통합사회, 통합과학', '9등급 상대평가 (1학년 과정)'],
        ['영어', '절대평가'],
        ['한국사', '절대평가'],
    ],
    col_widths=[9*cm, 6.5*cm]
))
elements += [sp(), h2('■ 핵심 시사점')]
elements += [
    bullet('변별력이 유지되는 과목: 수학, 국어'),
    bullet('각 대학 입학 전형 시 통합사회·통합과학 반영 비중 축소 예상'),
    bullet('영어, 한국사는 절대평가 → 3등급까지 변별력 없음'),
    sp(4),
    box('⇒ 수학, 국어가 결정하는 수능', GOLD, RED),
    sp(8),
]

# 2. 내신체제 변별력
elements += [h1('2. 새로운 내신체제의 변별력'), h2('■ 내신 체제의 변화')]
elements += [
    body('9등급제 (1등급 4%)  →  5등급제 (1등급 10%) : 등급이 나오는 과목 수 대폭 증가'),
    sp(),
]
elements.append(make_table(
    ['구분', '기존', '변경 후 (2028~)'],
    [
        ['공통과목', '상대평가(9등급)', '상대평가(5등급)'],
        ['일반선택', '상대평가(9등급)', '상대평가(5등급)'],
        ['진로선택', '절대평가', '상대평가(5등급)'],
        ['전문교과', '절대평가', '상대평가(5등급)'],
    ],
    col_widths=[4*cm, 5.5*cm, 6*cm]
))
elements += [sp(), h2('■ 5등급 변별력 확인 (실증 사례)')]
elements += [
    bullet('서울 고1 1학기 내신 전 과목 1등급 비율: 1.72% (1,009명)'),
    bullet('남은 과목 중 대부분이 선택과목 → 수강생 감소로 등급 경쟁 더욱 치열'),
    sp(4),
    box('⇒ 예상 밖의 역대급 변별력 입증', colors.HexColor('#FFE8CC'), colors.HexColor('#7B3F00')),
    sp(8),
]

# 3. 고교학점제
elements += [h1('3. 2022개정 교육과정과 고교학점제 전면 시행'), h2('■ 고교학점 배당 기준 (주요 교과)')]
elements.append(make_table(
    ['교과(군)', '필수 이수 학점'],
    [
        ['국어', '8'], ['수학', '8'], ['영어', '8'],
        ['사회 (역사/도덕 포함)', '6(한국사) / 8(통합사회)'],
        ['과학', '10 (통합과학, 과학탐구실험)'],
        ['체육', '10'],
    ],
    col_widths=[9*cm, 6.5*cm]
))
elements += [sp(), h2('■ 고교 학점제의 현실')]
elements += [
    bullet('수능과목 필수 수강'),
    bullet('내신 등급에 유리한 과목 개설'),
    bullet('학교 교육 편제 차이가 거의 없음'),
    sp(4),
    box('"진로에 따른 다양한 과목 선택"은 없다', LGRAY, NAVY),
    sp(8),
]

# 4. 무전공 입학
elements += [h1('4. 무전공 입학이란?'), h2('■ 유형 구분')]
elements += [
    bullet('유형1: 전공을 정하지 않고 모집 후 대학 내 모든 전공 자율 선택 (의전·의료·사범계열 등 제외)'),
    bullet('유형2: 계열 또는 단대 단위 모집 후 전공 자율 선택'),
    sp(), h2('■ 대학 인센티브 기준 (전체 정원 내 모집원 기준)'),
]
elements.append(make_table(
    ['학년도', '유형1', '유형1 + 유형2'],
    [
        ['2025학년도', '5%\n(2026학년도 10% 이상 계획 제출 시 인정)', '20% (수도권 사립대) / 25% (국립대)'],
        ['2026학년도', '10%', '25% (수도권 사립대) / 30% (국립대)'],
    ],
    col_widths=[3.5*cm, 6*cm, 6*cm]
))
elements.append(sp(8))

# 5. 2028 대입의 핵심
elements += [h1('5. 2028 대입의 핵심'), h2('■ 핵심 1: 고부터 대입은 시작된다')]
elements.append(make_table(
    ['전형', '현재', '2028~'],
    [
        ['수시', '학생부 + 수능', '학생부 + 수능'],
        ['정시', '수능', '학생부 + 수능'],
    ],
    col_widths=[3.5*cm, 6*cm, 6*cm]
))
elements += [sp()]
elements += [
    bullet('전형의 구분이 사라짐 → 내신과 수능 모두 잘해야'),
    bullet('패자부활전은 없다'),
    bullet('고등 3년 전 과정이 대입에 반영'),
    sp(), h2('■ 핵심 2: 수학·국어 비중 증가 (서울대 수능 반영 기준)'),
]
elements.append(make_table(
    ['과목', '반영 비율'],
    [
        ['국어', '100'],
        ['수학', '120 (최고 비중)'],
        ['영어', '등급별 감점 (1등급 0점 ~ 9등급 -14점)'],
        ['한국사', '반영 안 함'],
        ['통합과학·통합사회', '80'],
    ],
    col_widths=[7*cm, 8.5*cm]
))
elements.append(sp(8))

# 6. 최상위 초등 전략
elements += [h1('6. 최상위 초등의 전략')]
elements.append(make_table(
    ['전략', '경로', '비고'],
    [
        ['전략1 (메이저)', '초중등 → 자사고·명문고 전교권 → 명문대', '명문고 지역 전략 (주류)'],
        ['전략2 (마이너)', '초중등 → 영재고·과고 진학 → 명문대', '명문고 부재 지역 전략'],
    ],
    col_widths=[3.5*cm, 8*cm, 4*cm]
))
elements += [sp(), bullet('※ 인문계열 외고 진학은 여전히 유효'), sp(8)]

# 7. 의대목표 낙수효과
elements += [h1('7. 의대목표 낙수효과와 최상위권 진로 구조 변화'), h2('■ 최상위권 진로 구조의 변화')]
elements += [bullet("'이공계 최상위' '연구자·공학자' → 의치약 계열의 소득과 직업 안정성으로 이동"), sp()]
elements += [h2('■ 2027학년도 의치약계열 총 모집인원')]
elements.append(make_table(
    ['의대', '치대', '한의대', '약학대', '수의대', '지역의사', '합계'],
    [['4,485', '630', '725', '1,750', '496', '490', '8,575명']],
    col_widths=[2.5*cm, 2*cm, 2.5*cm, 2.5*cm, 2*cm, 2*cm, 2*cm]
))
elements += [sp(4), box('합계: 총 8,575명 (수능 응시생의 약 1.8%)', GOLD, RED)]
elements += [sp(), h2('■ 의대목표 낙수효과')]
elements += [bullet('모든 전형·모든 학과 절대반지 → 서울대 등 상위권대 진학 가능'), sp(8)]

# 8. 영재고 리스크
elements += [h1('8. 영재고 진학의 리스크'), h2('■ 리스크 요인')]
elements += [
    bullet('의대 진학 불리: 영재고 출신 의대 진학 제한'),
    bullet('영재고 대비 부담: 올림피아드 준비, 과학 올인, 시험 대비, 연구보고서 작성'),
    bullet('불합격 시 리스크: 자사고·일반고 전교권 불확실 (수학 선행·내신·수능 대비 부족, 국어·영어 열세)'),
    sp(), h2('■ 과고 진학 시'),
    bullet('과고는 카포디지유 전문 → SKY 첨단학과·계약학과 위주'),
    bullet('2026학년도 기준: 영재고·과고 의대 진학 40%대 급감'),
    sp(8),
]

# 9. 특목고 대안
elements += [h1('9. 특목고를 대신하는 효율적인 대안의 등장'), h2('■ 자사고 대폭 확대')]
elements += [
    bullet('2010년 이전: 지립형 사립고 (민사고, 광양제철고, 포항제철고 등)'),
    bullet('2010년 이후: 자율형 사립고 대폭 확대'),
    sp(), h2('■ 자사고·명문 일반고의 약진'),
    bullet('의대 진학에서 자사고·명문일반고 조강세'),
    bullet('효율적이고 안정적인 대안으로 등장'),
    sp(8),
]

# 10. 상위권 대학 구성
elements += [h1('10. 상위권 대학 모집인원 구성'), h2('■ 대학 서열 구조')]
elements.append(make_table(
    ['구분', '계층'],
    [
        ['①', '메이저·수도권 의대'],
        ['②', '지방의대, 치대'],
        ['③', '약대, 한의대, 서울대 자연계, 첨단학과, 인문계 상위권 학과'],
        ['④', '연고대 중하위권 학과'],
        ['⑤', '서성한이, 중경외시'],
    ],
    col_widths=[1.5*cm, 14*cm]
))
elements += [sp(4), box('자연계 약 11,000명 : 인문계 약 1,100명 (약 10:1)', BLUE, NAVY), sp(8)]

# 11. H고 과목선택
elements += [h1('11. H고 과목선택 → 주요 과목 비중 증가'), h2('■ 과목별 이수 목록 (H고 기준)')]
elements.append(make_table(
    ['교과', '이수 과목'],
    [
        ['국어 (8과목)', '공통국어1·2 / 문학 / 독서와 작문 / 화법과 언어 / 언어생활과 탐구 / 매체의사와소통 / 독서토론과 글쓰기'],
        ['영어 (5과목)', '공통영어1·2 / 영어1·2 / 영어 독해와 작문'],
        ['수학 (10과목)', '공통수학1·2 / 대수 / 미적분Ⅰ / 확률과 통계 / 기하(경제수학) / 미적분Ⅱ / 고급 미적분 / 인공지능수학 / 수학과제탐구'],
        ['과학 (8.5과목)', '통합과학1·2 / 물리학 / 화학 / 생명과학 / 역학과 에너지 / 화학반응의 세계 / 물질과 에너지 / 세포와 물질대사 / 생물의 유전 / 고급 화학'],
    ],
    col_widths=[3.5*cm, 12*cm]
))
elements += [sp(4), box('"내신도 수능도" 수학 비중이 절대적', GOLD, RED), sp(8)]

# 12. 수학 이수 전략
elements += [h1('12. 고교 진학 후 수학 이수 전략'), h2('■ 핵심 원칙: 내신 상위권 선행이 필수')]
elements += [
    bullet('무거운 과목(수능과목): 수능과목 완성, 내신문제=수능문제, 겨울방학 수능 준비'),
    bullet('심화 과목: 심화 과목 이수 경쟁, 대학 권장 과목 이수'),
    bullet('미적분Ⅱ, 기하 선행 필수'),
    sp(), h2('■ H고 / M고 수학 이수 예시'),
]
elements.append(make_table(
    ['학교', '1-1', '1-2', '2-1', '2-2', '3-1', '3-2'],
    [
        ['H고', '공통수학1', '공통수학2', '대수', '미적분Ⅰ', '미적분Ⅱ/확률과통계/기하 등', '인공지능수학/고급미적분 등'],
        ['M고', '공통수학1', '공통수학2', '대수', '미적분Ⅰ/기하', '확률과통계/미적분Ⅱ/경제수학', '수학과제탐구/실용통계 등'],
    ],
    col_widths=[1.8*cm, 2.2*cm, 2.2*cm, 2.2*cm, 2.2*cm, 2.9*cm, 2*cm]
))
elements.append(sp(8))

# 13. 구주이배 커리큘럼
elements += [h1('13. 구주이배 수준별 수학 커리큘럼'), h2('■ 고등학교 진학 전 수준별 커리큘럼 (Z·M·G 트랙)')]
elements.append(make_table(
    ['레벨', '공통수학', '내수', '미적분Ⅰ', '확률과통계', '기하', '미적분Ⅱ'],
    [
        ['Z',  '내신실전', '수능실전', '수능실전', '수능실전', '기본', '기본'],
        ['M',  '내신실전', '수능실전', '수능실전', '심화', '기본', '-'],
        ['M',  '내신실전', '심화', '심화', '기본', '-', '-'],
        ['G',  '내신실전', '심화', '기본', '-', '-', '-'],
        ['G',  '내신실전', '기본', '기본', '-', '-', '-'],
        ['G',  '내신실전', '기본', '-', '-', '-', '-'],
    ],
    col_widths=[1.5*cm, 2.5*cm, 2*cm, 2.5*cm, 2.5*cm, 2*cm, 2.5*cm]
))
elements += [sp(), bullet('선행 필수: 상위권 결정 과목 = 고등 주요 내신 결정 = 수능과목')]
elements += [bullet('최신 선행 전략: 대수·미적분·확통·심화 중심'), sp()]
elements += [h2('■ 수준별 수학 커리큘럼 (초6~중3 시기별)')]
elements.append(make_table(
    ['레벨', '시작 시점', '고등과정 준비 기간'],
    [
        ['Z', '초6에 공수1 시작 → 중1부터 고등과정', '4년'],
        ['M', '중등과정 + 공수1 → 고등과정', '3~3.5년'],
        ['G', '중등과정 → 공수1 (중2~중3)', '2~2.5년'],
    ],
    col_widths=[2*cm, 9.5*cm, 4*cm]
))
elements += [sp(4), box('핵심: 고등과정 시작이 선행 결과를 결정한다\n상위 20개 대학 목표 시 고등선행 최소 2년 확보 필요', GREEN, colors.HexColor('#1A5C1A')), sp(8)]

# 14. 학습량과 난이도
elements += [h1('14. 수학 과정별 학습량과 난이도'), h2('■ 단계적 학습 구조 (학습량↑, 난이도↑)')]
elements.append(make_table(
    ['단계', '학습 내용'],
    [
        ['1단계', '초등과정 기초'],
        ['2단계', '중등과정 기초'],
        ['3단계', '고등과정 개념 및 유형'],
        ['4단계', '고등내신 1등급 문제해결 학습'],
        ['5단계 (최상위)', '수능 1등급 문제해결 학습'],
    ],
    col_widths=[4*cm, 11.5*cm]
))
elements.append(sp(20))
elements.append(Paragraph('─ 끝 ─', ParagraphStyle('end', fontName=FONT, fontSize=11,
               textColor=colors.HexColor('#808080'), alignment=TA_CENTER)))

# ── 빌드 ────────────────────────────────────────────────────────
doc = SimpleDocTemplate(
    '/home/user/ai2/2028대입_교육전략_보고자료.pdf',
    pagesize=A4,
    leftMargin=2.5*cm, rightMargin=2.5*cm,
    topMargin=2.5*cm, bottomMargin=2.5*cm,
)
doc.build(elements)
print("PDF 저장 완료")
