# 클로드로 요청한 내용으로 01. 번 내용에서 (3) 병합부분만 수정요청하니 group by로 수정함

import streamlit as st
import pandas as pd
import io
from typing import Dict, List

def process_ecount_file(df: pd.DataFrame) -> pd.DataFrame:
    """
    이카운트 엑셀 파일을 홈택스 업로드 양식으로 변환합니다.
    
    Args:
        df (pd.DataFrame): 원본 이카운트 데이터프레임.
        
    Returns:
        pd.DataFrame: 변환된 홈택스 양식의 데이터프레임.
    """
    # 1. 데이터 전처리
    df['code'] = '01'  # 유형: 01 (일반세금계산서)
    df['Date'] = df['Date'].astype(str).str[:8]
    df['day'] = df['Date'].str[-2:]
    df['TaxNo_Send'] = df['TaxNo_Send'].astype(str)
    df['TaxNo_get'] = df['TaxNo_get'].astype(str)

    # 2. 공급가액이 0보다 큰 데이터만 선택
    df = df[df['price'] > 0]
    
    # 3. 개선된 데이터 병합
    # 기준이 되는 키 컬럼 정의
    key_columns = ['code', 'Date', 'TaxNo_Send', 'J1', 'Title_send', 'Name_send',
                   'Addr_send', 'sub1', 'sub2', 'Email_send',
                   'TaxNo_get', 'J2', 'TaxTitle_get', 'Name_get',
                   'Addr_get', 'type1', 'type2', 'Email_get', 'Email2_get', 'note_Sum']

    # 품목 매핑 정의 (우선순위 순으로 정렬)
    item_mapping = {
        '임대료': 1,
        '관리비': 2,
        '전기료': 3,
        '주차료': 4
    }
    
    # 하나은행 데이터를 임대료로 처리
    df.loc[df['TaxNo_get'] == '2298500670', 'item'] = '임대료'
    
    # 품목별로 데이터를 그룹화하고 피벗 테이블 생성
    merged_df = create_pivot_table(df, key_columns, item_mapping)
    
    # 4. 합계 계산
    merged_df = calculate_totals(merged_df)
    
    # 5. 홈택스 양식에 맞게 열 순서 재정렬 및 추가
    final_df = format_final_output(merged_df)
    
    return final_df


def create_pivot_table(df: pd.DataFrame, key_columns: List[str], item_mapping: Dict[str, int]) -> pd.DataFrame:
    """
    품목별 데이터를 피벗 테이블로 변환하여 병합합니다.
    """
    # 필요한 데이터 컬럼
    value_columns = ['day', 'item', 'standard', 'quantity', 'unit_price', 'price', 'VAT', 'note']
    
    # 매핑에 있는 품목만 필터링
    df_filtered = df[df['item'].isin(item_mapping.keys())].copy()
    
    # 품목별 우선순위 추가
    df_filtered['item_priority'] = df_filtered['item'].map(item_mapping)
    
    # 키 컬럼과 값 컬럼 결합
    all_columns = key_columns + value_columns + ['item_priority']
    df_work = df_filtered[all_columns].copy()
    
    # 각 키 조합별로 품목들을 하나의 행으로 병합
    result_rows = []
    
    for key_values, group in df_work.groupby(key_columns):
        # 키 컬럼들의 기본값 설정
        row_dict = dict(zip(key_columns, key_values))
        
        # 우선순위 순으로 정렬
        group_sorted = group.sort_values('item_priority')
        
        # 각 품목별 데이터를 suffix와 함께 저장
        for idx, (_, item_row) in enumerate(group_sorted.iterrows(), 1):
            if idx <= 4:  # 최대 4개 품목까지만 처리
                for col in value_columns:
                    if col in item_row:
                        row_dict[f'{col}_{idx}'] = item_row[col]
                    else:
                        row_dict[f'{col}_{idx}'] = ''
        
        result_rows.append(row_dict)
    
    return pd.DataFrame(result_rows)


def calculate_totals(df: pd.DataFrame) -> pd.DataFrame:
    """
    품목별 가격과 VAT의 합계를 계산합니다.
    """
    price_cols = [f'price_{i}' for i in range(1, 5)]
    vat_cols = [f'VAT_{i}' for i in range(1, 5)]
    
    # 컬럼이 없는 경우 0으로 생성
    for col in price_cols + vat_cols:
        if col not in df.columns:
            df[col] = 0
    
    # NaN 값을 0으로 채우고 숫자형으로 변환
    for col in price_cols + vat_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # 합계 계산
    df['price_sum'] = df[price_cols].sum(axis=1).astype(int)
    df['VAT_sum'] = df[vat_cols].sum(axis=1).astype(int)
    
    return df


def format_final_output(df: pd.DataFrame) -> pd.DataFrame:
    """
    홈택스 양식에 맞게 최종 출력 포맷을 설정합니다.
    """
    # 최종 컬럼 순서 정의
    final_columns = [
        'code', 'Date', 'TaxNo_Send', 'J1', 'Title_send', 'Name_send', 'Addr_send', 
        'sub1', 'sub2', 'Email_send', 'TaxNo_get', 'J2', 'TaxTitle_get', 'Name_get', 
        'Addr_get', 'type1', 'type2', 'Email_get', 'Email2_get', 'price_sum', 
        'VAT_sum', 'note_Sum'
    ]
    
    # 품목별 컬럼 추가 (1-4번)
    for i in range(1, 5):
        final_columns.extend([
            f'day_{i}', f'item_{i}', f'standard_{i}', f'quantity_{i}', 
            f'unit_price_{i}', f'price_{i}', f'VAT_{i}', f'note_{i}'
        ])
    
    # 없는 컬럼은 빈 값으로 추가
    for col in final_columns:
        if col not in df.columns:
            df[col] = ''
    
    # 최종 데이터프레임 생성
    df_final = df[final_columns].copy()
    
    # 추가 데이터 정리
    for i in range(1, 5):
        df_final[f'note_{i}'] = ''
    
    # 기타 필드 추가
    df_final["etc1"] = ""
    df_final["etc2"] = ""
    df_final["etc3"] = ""
    df_final["etc4"] = ""
    df_final["etc5"] = "02"  # 청구(02)
    
    # 사업자번호 정리
    df_final['TaxNo_get'] = df_final['TaxNo_get'].str.replace('_B', '', regex=False)
    
    # NaN 값을 빈 문자열로 변환
    df_final = df_final.fillna('')
    
    return df_final


# --- Streamlit App UI ---
st.set_page_config(page_title="홈택스 세금계산서 변환기", layout="wide")
st.title("📄 이카운트 엑셀 → 홈택스 업로드 양식 변환기")
st.info("이카운트 '판매현황(거래처품목별-TAX1양식)' 엑셀 파일을 홈택스 대량 발행 양식으로 변환합니다.")

uploaded_file = st.file_uploader("📂 이카운트 엑셀 파일을 업로드하세요", type=["xlsx", "xls"])

if uploaded_file:
    st.success(f"파일이 성공적으로 업로드되었습니다: **{uploaded_file.name}**")
    
    try:
        # 엑셀 파일 로드 (양식에 맞게 첫 행은 건너뛰고, 마지막 2개 행은 제외)
        df_original = pd.read_excel(uploaded_file, skiprows=1, skipfooter=2, header=0)
        
        # 사용자가 원본 데이터를 확인할 수 있도록 expander 안에 미리보기 제공
        with st.expander("📂 업로드한 원본 파일 미리보기"):
            st.dataframe(df_original)

        if st.button("🚀 변환 실행", use_container_width=True):
            with st.spinner('데이터를 변환하는 중입니다... 잠시만 기다려주세요.'):
                # 데이터 변환 함수 호출 (원본 보존을 위해 복사본 전달)
                processed_df = process_ecount_file(df_original.copy())

                st.subheader("✅ 변환 결과 미리보기")
                st.dataframe(processed_df)

                # 엑셀 파일 다운로드를 위해 인메모리 버퍼에 저장
                output = io.BytesIO()
                # 홈택스 양식에 맞게 5행 아래부터 데이터를 작성
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    processed_df.to_excel(writer, sheet_name='sale1', index=False, startrow=5)
                
                excel_data = output.getvalue()

                st.download_button(
                    label="📥 'tax_upload.xlsx' 파일 다운로드",
                    data=excel_data,
                    file_name="tax_upload.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )

    except Exception as e:
        st.error(f"파일을 처리하는 중 오류가 발생했습니다: {e}")
        st.warning("업로드한 파일이 '판매현황(거래처품목별-TAX1양식)'이 맞는지 확인해주세요.")

else:
    st.info("파일을 업로드하면 변환을 시작할 수 있습니다.")
