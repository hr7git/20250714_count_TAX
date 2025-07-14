import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
import base64

def read_csv_file(uploaded_file):
    """CSV 파일을 읽어서 DataFrame으로 반환"""
    try:
        # 다양한 인코딩 시도
        encodings = ['utf-8', 'cp949', 'euc-kr', 'latin-1']
        
        for encoding in encodings:
            try:
                uploaded_file.seek(0)  # 파일 포인터 초기화
                df = pd.read_csv(uploaded_file, encoding=encoding)
                return df
            except UnicodeDecodeError:
                continue
        
        # 모든 인코딩이 실패한 경우
        st.error("파일을 읽을 수 없습니다. 파일 인코딩을 확인해주세요.")
        return None
        
    except Exception as e:
        st.error(f"파일을 읽는 중 오류가 발생했습니다: {str(e)}")
        return None

def transform_input_to_output(df):
    """입력 데이터를 출력 형태로 변환"""
    try:
        # 헤더 제거 (첫 번째 행이 회사 정보인 경우)
        if df.iloc[0, 0] and '회사명' in str(df.iloc[0, 0]):
            df = df.iloc[1:].reset_index(drop=True)
        
        # 마지막 행들 제거 (총계 등)
        df = df[df['code'].notna() & (df['code'] != '')]
        df = df[~df['code'].astype(str).str.contains('총합계|2025/', na=False)]
        
        # TaxNo_get별로 그룹화
        grouped = df.groupby('TaxNo_get')
        
        result_rows = []
        
        for tax_no, group in grouped:
            # 그룹 내 첫 번째 행의 기본 정보 가져오기
            first_row = group.iloc[0]
            
            # 새로운 행 생성
            new_row = {
                'code': '01',
                'Date': first_row['Date'],
                'TaxNo_Send': first_row['TaxNo_Send'],
                'J1': first_row['J1'],
                'Title_send': first_row['Title_send'],
                'Name_send': first_row['Name_send'],
                'Addr_send': first_row['Addr_send'],
                'sub1': first_row['sub1'],
                'sub2': first_row['sub2'],
                'Email_send': first_row['Email_send'],
                'TaxNo_get': first_row['TaxNo_get'],
                'J2': first_row['J2'],
                'TaxTitle_get': first_row['TaxTitle_get'],
                'Name_get': first_row['Name_get'],
                'Addr_get': first_row['Addr_get'],
                'type1': first_row['type1'],
                'type2': first_row['type2'],
                'Email_get': first_row['Email_get'],
                'Email2_get': first_row['Email2_get'],
                'note_Sum': first_row['note_Sum']
            }
            
            # price와 VAT 합계 계산
            total_price = group['price'].astype(str).str.replace(',', '').astype(float).sum()
            total_vat = group['VAT'].astype(str).str.replace(',', '').astype(float).sum()
            
            new_row['price_sum'] = int(total_price)
            new_row['VAT_sum'] = int(total_vat)
            
            # 각 항목별 데이터 추가
            items = group.reset_index(drop=True)
            for i, item in items.iterrows():
                idx = i + 1
                new_row[f'day_{idx}'] = item['day']
                new_row[f'item_{idx}'] = item['item']
                new_row[f'standard_{idx}'] = item['standard']
                new_row[f'quantity_{idx}'] = item['quantity']
                new_row[f'unit_price_{idx}'] = item['unit_price']
                
                # 숫자 형식 처리
                price_val = str(item['price']).replace(',', '') if pd.notna(item['price']) else '0'
                vat_val = str(item['VAT']).replace(',', '') if pd.notna(item['VAT']) else '0'
                
                new_row[f'price_{idx}'] = int(float(price_val)) if price_val else 0
                new_row[f'VAT_{idx}'] = int(float(vat_val)) if vat_val else 0
                new_row[f'note_{idx}'] = item['note']
                
                if idx >= 4:  # 최대 4개 항목까지만
                    break
            
            # 빈 필드들 채우기
            for i in range(1, 5):
                for field in ['day', 'item', 'standard', 'quantity', 'unit_price', 'price', 'VAT', 'note']:
                    if f'{field}_{i}' not in new_row:
                        new_row[f'{field}_{i}'] = ''
            
            # etc 필드들 추가
            for i in range(1, 6):
                new_row[f'etc{i}'] = '02' if i == 5 else ''
            
            result_rows.append(new_row)
        
        return pd.DataFrame(result_rows)
        
    except Exception as e:
        st.error(f"데이터 변환 중 오류가 발생했습니다: {str(e)}")
        return None

def download_csv(df, filename):
    """DataFrame을 CSV로 다운로드"""
    output = BytesIO()
    df.to_csv(output, index=False, encoding='utf-8-sig')
    output.seek(0)
    
    b64 = base64.b64encode(output.read()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">📥 {filename} 다운로드</a>'
    return href

def main():
    st.set_page_config(page_title="Invoice Data Transformer", page_icon="📊", layout="wide")
    
    st.title("📊 Invoice Data Transformer")
    st.markdown("세금계산서 데이터를 입력 형태에서 출력 형태로 변환하는 도구입니다.")
    
    # 사이드바
    st.sidebar.title("📁 파일 업로드")
    uploaded_file = st.sidebar.file_uploader(
        "CSV 파일을 선택하세요",
        type=['csv'],
        help="입력 형태의 CSV 파일을 업로드하세요"
    )
    
    if uploaded_file is not None:
        st.sidebar.success("✅ 파일이 업로드되었습니다!")
        
        # 파일 읽기
        df = read_csv_file(uploaded_file)
        
        if df is not None:
            # 탭 생성
            tab1, tab2, tab3 = st.tabs(["📋 입력 데이터", "🔄 변환된 데이터", "📊 요약"])
            
            with tab1:
                st.header("📋 입력 데이터 미리보기")
                st.dataframe(df, use_container_width=True)
                
                # 기본 정보 표시
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("총 행 수", len(df))
                with col2:
                    st.metric("총 컬럼 수", len(df.columns))
                with col3:
                    unique_companies = df['TaxNo_get'].nunique() if 'TaxNo_get' in df.columns else 0
                    st.metric("고유 회사 수", unique_companies)
            
            with tab2:
                st.header("🔄 변환된 데이터")
                
                # 변환 버튼
                if st.button("🔄 데이터 변환 실행", type="primary"):
                    with st.spinner("데이터를 변환하는 중..."):
                        transformed_df = transform_input_to_output(df)
                    
                    if transformed_df is not None:
                        st.success("✅ 변환이 완료되었습니다!")
                        
                        # 변환된 데이터 표시
                        st.dataframe(transformed_df, use_container_width=True)
                        
                        # 다운로드 버튼
                        st.markdown("### 📥 다운로드")
                        csv_data = transformed_df.to_csv(index=False, encoding='utf-8-sig')
                        st.download_button(
                            label="📥 변환된 데이터 다운로드 (CSV)",
                            data=csv_data,
                            file_name="output_transformed.csv",
                            mime="text/csv",
                            type="primary"
                        )
                        
                        # 세션 상태에 저장
                        st.session_state.transformed_df = transformed_df
            
            with tab3:
                st.header("📊 데이터 요약")
                
                if 'transformed_df' in st.session_state:
                    transformed_df = st.session_state.transformed_df
                    
                    # 요약 통계
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.subheader("📈 금액 통계")
                        total_price = transformed_df['price_sum'].sum()
                        total_vat = transformed_df['VAT_sum'].sum()
                        
                        st.metric("총 공급가액", f"₩{total_price:,}")
                        st.metric("총 부가세", f"₩{total_vat:,}")
                        st.metric("총 계", f"₩{total_price + total_vat:,}")
                    
                    with col2:
                        st.subheader("🏢 회사별 통계")
                        company_stats = transformed_df.groupby('TaxTitle_get').agg({
                            'price_sum': 'sum',
                            'VAT_sum': 'sum'
                        }).reset_index()
                        
                        for _, row in company_stats.iterrows():
                            with st.expander(f"🏢 {row['TaxTitle_get']}"):
                                st.write(f"공급가액: ₩{row['price_sum']:,}")
                                st.write(f"부가세: ₩{row['VAT_sum']:,}")
                                st.write(f"합계: ₩{row['price_sum'] + row['VAT_sum']:,}")
                else:
                    st.info("변환된 데이터가 없습니다. 먼저 데이터를 변환해주세요.")
    
    else:
        st.info("👆 왼쪽 사이드바에서 CSV 파일을 업로드해주세요.")
        
        # 샘플 데이터 형식 안내
        st.markdown("### 📋 입력 데이터 형식")
        st.markdown("""
        업로드할 CSV 파일은 다음과 같은 컬럼들을 포함해야 합니다:
        - code, Date, TaxNo_Send, Title_send, Name_send, etc.
        - 각 행은 세금계산서의 개별 항목을 나타냅니다.
        """)
        
        st.markdown("### 🔄 변환 결과")
        st.markdown("""
        변환된 데이터는 다음과 같은 형태로 출력됩니다:
        - 같은 TaxNo_get을 가진 항목들이 하나의 행으로 통합됩니다.
        - 각 항목은 item_1, item_2, ... 형태로 저장됩니다.
        - price_sum, VAT_sum이 자동으로 계산됩니다.
        """)

if __name__ == "__main__":
    main()