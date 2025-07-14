import streamlit as st
import pandas as pd
import io

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

    # 2. 공급가액이 0보다 큰 데이터만 선택 및 품목별 분리
    df = df[df['price'] > 0]
    
    # '하나은행' 데이터 별도 처리
    df_Hana = df[df['TaxNo_get'] == '2298500670']
    df = df[df['TaxNo_get'] != '2298500670']

    # 품목별 DataFrame 생성
    df1 = df[df['item'] == '임대료']
    df2 = df[df['item'] == '관리비']
    df3 = df[df['item'] == '전기료']
    df4 = df[df['item'] == '주차료']
    
    # 분리했던 '하나은행' 데이터를 '임대료'에 다시 포함
    if not df_Hana.empty:
        df1 = pd.concat([df_Hana, df1])

    # 3. 데이터 병합
    # 기준이 되는 키 컬럼 정의
    key_id = ['code', 'Date', 'TaxNo_Send', 'J1', 'Title_send', 'Name_send',
              'Addr_send', 'sub1', 'sub2', 'Email_send',
              'TaxNo_get', 'J2', 'TaxTitle_get', 'Name_get',
              'Addr_get', 'type1', 'type2', 'Email_get', 'Email2_get', 'note_Sum']

    # 품목별 데이터를 key_id 기준으로 병합
    merged_df = pd.merge(df1, df2, how='outer', on=key_id, suffixes=('_1', '_2'))
    merged_df = pd.merge(merged_df, df3, how='outer', on=key_id)
    merged_df = pd.merge(merged_df, df4, how='outer', on=key_id, suffixes=('_3', '_4'))
    
    # 4. 합계 계산 및 추가
    price_cols = ['price_1', 'price_2', 'price_3', 'price_4']
    vat_cols = ['VAT_1', 'VAT_2', 'VAT_3', 'VAT_4']
    
    # DataFrame에 해당 열이 없을 경우 생성
    for col in price_cols + vat_cols:
        if col not in merged_df.columns:
            merged_df[col] = 0

    # NaN 값을 0으로 채우고 합계 계산
    merged_df.loc[:, 'price_sum'] = merged_df[price_cols].fillna(0).sum(axis=1).astype(int)
    merged_df.loc[:, 'VAT_sum'] = merged_df[vat_cols].fillna(0).sum(axis=1).astype(int)
    
    # 5. 홈택스 양식에 맞게 열 순서 재정렬 및 추가
    # 최종적으로 필요한 열 목록
    final_columns = [
        'code', 'Date', 'TaxNo_Send', 'J1', 'Title_send', 'Name_send', 'Addr_send', 
        'sub1', 'sub2', 'Email_send', 'TaxNo_get', 'J2', 'TaxTitle_get', 'Name_get', 
        'Addr_get', 'type1', 'type2', 'Email_get', 'Email2_get', 'price_sum', 
        'VAT_sum', 'note_Sum', 'day_1', 'item_1', 'standard_1', 'quantity_1', 
        'unit_price_1', 'price_1', 'VAT_1', 'note_1', 'day_2', 'item_2', 
        'standard_2', 'quantity_2', 'unit_price_2', 'price_2', 'VAT_2', 'note_2', 
        'day_3', 'item_3', 'standard_3', 'quantity_3', 'unit_price_3', 'price_3', 
        'VAT_3', 'note_3', 'day_4', 'item_4', 'standard_4', 'quantity_4', 
        'unit_price_4', 'price_4', 'VAT_4', 'note_4'
    ]
    
    # 없는 열은 빈 값으로 추가
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
    
    if st.button("🚀 변환 실행", use_container_width=True):
        try:
            # 엑셀 파일 로드 (양식에 맞게 첫 행은 건너뛰고, 마지막 2개 행은 제외)
            df_original = pd.read_excel(uploaded_file, skiprows=1, skipfooter=2, header=0)
            
            with st.spinner('데이터를 변환하는 중입니다... 잠시만 기다려주세요.'):
                # 데이터 변환 함수 호출
                processed_df = process_ecount_file(df_original)

                st.subheader("✅ 변환 결과 미리보기")
                st.dataframe(processed_df)

                # 엑셀 파일 다운로드를 위해 인메모리 버퍼에 저장
                output = io.BytesIO()
                # 홈택스 양식에 맞게 5행 아래부터 데이터를 작성
                # openpyxl 엔진이 startrow를 지원
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
            st.error(f"오류가 발생했습니다: {e}")
            st.warning("업로드한 파일이 '판매현황(거래처품목별-TAX1양식)'이 맞는지 확인해주세요.")

else:
    st.info("파일을 업로드하면 변환을 시작할 수 있습니다.")
