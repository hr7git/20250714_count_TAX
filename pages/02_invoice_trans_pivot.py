#  01_inove_transformer.py 를  3번 병합분을  gemini 로 수정요청하니
#  merge 대신에 pivot 으로 바꾸어주었음 but 에러발생해서  claude 로 새로 요청함.
import streamlit as st
import pandas as pd
import io

def process_ecount_file(df: pd.DataFrame) -> pd.DataFrame:
    """
    이카운트 엑셀 파일을 홈택스 업로드 양식으로 변환합니다.
    (데이터 병합 로직 개선)
    """
    # 1. 데이터 전처리
    df['code'] = '01'  # 유형: 01 (일반세금계산서)
    df['Date'] = df['Date'].astype(str).str[:8]
    df['day'] = df['Date'].str[-2:]
    df['TaxNo_Send'] = df['TaxNo_Send'].astype(str)
    df['TaxNo_get'] = df['TaxNo_get'].astype(str)

    # 2. 유효 데이터 필터링
    df = df[df['price'] > 0].copy() # .copy()를 사용하여 SettingWithCopyWarning 방지

    # --- 3. 데이터 병합 (개선된 로직: pivot_table 사용) ---
    # 기준이 되는 키 컬럼 정의 (거래처별로 고유한 값을 가지는 열)
    key_id = ['code', 'Date', 'TaxNo_Send', 'J1', 'Title_send', 'Name_send',
              'Addr_send', 'sub1', 'sub2', 'Email_send',
              'TaxNo_get', 'J2', 'TaxTitle_get', 'Name_get',
              'Addr_get', 'type1', 'type2', 'Email_get', 'Email2_get', 'note_Sum']
    
    # 피벗할 때 열로 변환될 값들이 담긴 컬럼
    # 원본 파일에 standard, quantity, unit_price 등이 없을 수 있으므로 확인 후 추가
    pivot_values = ['day', 'item', 'standard', 'quantity', 'unit_price', 'price', 'VAT', 'note', 'Title_get']
    for col in pivot_values:
        if col not in df.columns:
            df[col] = '' # 없는 경우 빈 값으로 컬럼 생성

    # STEP 1: 각 거래처(key_id) 내에서 품목별 순번(1, 2, 3, 4...)을 매깁니다.
    df['item_num'] = df.groupby(key_id).cumcount() + 1
    
    # 4개 품목 이상은 제외 (필요시 주석 처리 또는 숫자 변경)
    df = df[df['item_num'] <= 4]

    # STEP 2: pivot_table을 사용하여 데이터를 Long에서 Wide 포맷으로 변환합니다.
    # index: 고유 행의 기준이 될 컬럼
    # columns: 열로 변환될 값 (품목 순번)
    # values: 피벗되어 각 열에 채워질 값
    merged_df = df.pivot_table(
        index=key_id,
        columns='item_num',
        values=pivot_values,
        aggfunc='first' # 각 그룹에는 하나의 값만 존재하므로 'first' 사용
    )

    # STEP 3: 다중 레벨 컬럼을 단일 레벨로 변환합니다. (예: ('price', 1) -> 'price_1')
    merged_df.columns = [f'{val}_{num}' for val, num in merged_df.columns]
    merged_df = merged_df.reset_index()
    # --- 개선된 로직 종료 ---

    # 4. 합계 계산 및 추가
    price_cols = [f'price_{i}' for i in range(1, 5)]
    vat_cols = [f'VAT_{i}' for i in range(1, 5)]
    
    for col in price_cols + vat_cols:
        if col not in merged_df.columns:
            merged_df[col] = 0

    merged_df['price_sum'] = merged_df[price_cols].fillna(0).sum(axis=1).astype(int)
    merged_df['VAT_sum'] = merged_df[vat_cols].fillna(0).sum(axis=1).astype(int)

    # 5. 홈택스 양식에 맞게 열 순서 재정렬 및 추가
    final_columns = [
        'code', 'Date', 'TaxNo_Send', 'J1', 'Title_send', 'Name_send', 'Addr_send', 
        'sub1', 'sub2', 'Email_send', 'TaxNo_get', 'J2', 'TaxTitle_get', 'Name_get', 
        'Addr_get', 'type1', 'type2', 'Email_get', 'Email2_get', 'price_sum', 
        'VAT_sum', 'note_Sum'] + [f'{c}_{i}' for i in range(1, 5) for c in pivot_values]
    
    for col in final_columns:
        if col not in merged_df.columns:
            merged_df[col] = ''
            
    df_final = merged_df[final_columns]
    
    # 6. 추가 데이터 정리
    for i in range(1, 5):
        df_final.loc[:, f'note_{i}'] = ''
        
    df_final.loc[:, "etc1"] = ""
    df_final.loc[:, "etc2"] = ""
    df_final.loc[:, "etc3"] = ""
    df_final.loc[:, "etc4"] = ""
    df_final.loc[:, "etc5"] = "02"  # 청구(02)
    
    df_final.loc[:, 'TaxNo_get'] = df_final['TaxNo_get'].str.replace('_B', '', regex=False)
    
    return df_final.fillna('')


# --- Streamlit App UI ---
st.set_page_config(page_title="홈택스 세금계산서 변환기", layout="wide")
st.title("📄 이카운트 엑셀 → 홈택스 업로드 양식 변환기")
st.info("이카운트 '판매현황(거래처품목별-TAX1양식)' 엑셀 파일을 홈택스 대량 발행 양식으로 변환합니다.")

uploaded_file = st.file_uploader("📂 이카운트 엑셀 파일을 업로드하세요", type=["xlsx", "xls"])

if uploaded_file:
    st.success(f"파일이 성공적으로 업로드되었습니다: **{uploaded_file.name}**")
    
    try:
        df_original = pd.read_excel(uploaded_file, skiprows=1, skipfooter=2, header=0)
        
        with st.expander("📂 업로드한 원본 파일 미리보기"):
            st.dataframe(df_original)

        if st.button("🚀 변환 실행", use_container_width=True):
            with st.spinner('데이터를 변환하는 중입니다... 잠시만 기다려주세요.'):
                processed_df = process_ecount_file(df_original.copy())

                st.subheader("✅ 변환 결과 미리보기")
                st.dataframe(processed_df)

                output = io.BytesIO()
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
