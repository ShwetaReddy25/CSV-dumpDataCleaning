import pandas as pd
import warnings
from datetime import datetime

try:
    warnings.simplefilter(action="ignore", category=pd.errors.SettingWithCopyWarning)
except Exception:
    warnings.simplefilter(action="ignore", category=Warning)

month_input = 12
year_input = 2025
cutoff_date = datetime(year_input, month_input, 9, 23, 59, 59)

input_file = "data/daily.csv"
print(f"Using CSV file: {input_file}")
df = pd.read_csv(input_file)
raw_dump_df = df.copy()

df["Business"] = ""
intl_countries = ["Egypt", "Turkey", "Nepal", "Malyasia", "Uzbekistan", "Malaysia",
                  "Jordan", "Türkiye", "TÃ¼rkiye", "EGYPT", "EGPYT", "UAE"]

df.loc[(df["Business"] == "") & df["Country"].isin(intl_countries), "Business"] = "International"
df.loc[(df["Business"] == "") & (df["Facility/Hospital Name"] == "Cancer institute W.I.A"), "Business"] = "Non-Service"
df.loc[(df["Business"] == "") & (df["Order Created By"] == "indx2.bot@indx.ai"), "Business"] = "Non-Service"

foc_users = [
    "sharmada.wagle@onecelldx.com", "priti.thate@onecelldx.com",
    "rajarshi.bhattacharjee@onecelldx.com", "navya.nandiraju@onecelldx.com",
    "snehal.kathwate@onecelldx.com"
]
df.loc[(df["Business"] == "") & (df["Payment Status"] == "FOC") & df["Order Created By"].isin(foc_users), "Business"] = "Non-Service"
df.loc[(df["Business"] == "") & (df["Payment Status"] == "FOC") & (~df["Order Created By"].isin(foc_users)) & (df["Order Created By"] != "indx2.bot@indx.ai"), "Business"] = "Service FOC"

df.loc[(df["Business"] == "") & (
    df["Sample Category"].astype(str).str.contains("Service", case=False, na=False)
), "Business"] = "Service"
df.loc[df["Business"] == "", "Business"] = "Non-Service"

df["Payment Type"] = ""
df.loc[df["Order Type"] == "MOU", "Payment Type"] = "B2B"
df.loc[df["Order Type"] == "Retail", "Payment Type"] = "B2C"
df.loc[df["Order Type"] == "FOC", "Payment Type"] = "FOC"
df.loc[df["Payment Status"] == "FOC", "Payment Type"] = "FOC"
df.loc[df["Order Type"].astype(str).str.contains("Research", na=False), "Payment Type"] = "Other"
valid_payment_types = ["B2B", "B2C", "Other"]

# ======= FIXED PART =======
asm_df = pd.read_excel("data/email grouping updated.xlsx", engine="openpyxl")
asm_df.columns = asm_df.columns.astype(str).str.strip()

# AUTO FIND EMAIL COLUMN
possible_email_cols = [c for c in asm_df.columns if "email" in c.lower() and "id" in c.lower()]
if len(possible_email_cols) == 0:
    raise Exception("Email column not found in ASM Excel file. Please check column names.")

email_col = possible_email_cols[0]
asm_df.rename(columns={email_col: "Order Created By"}, inplace=True)

# Now rename ASM/Region columns if exist
if "ASM NAME" in asm_df.columns:
    asm_df.rename(columns={"ASM NAME": "ASM"}, inplace=True)
if "Region" not in asm_df.columns:
    asm_df["Region"] = None

asm_map = asm_df.drop_duplicates("Order Created By").set_index("Order Created By")[["ASM", "Region"]]
df = df.merge(asm_map, on="Order Created By", how="left")

# ======= CONTINUE REST OF CODE (unchanged) =======
ilms_df = pd.read_excel("data/ilims data grouping (3).xlsx", engine="openpyxl")
ilms_df.columns = ilms_df.columns.astype(str).str.strip()

if "Doctor Name" in ilms_df.columns:
    ilms_df.rename(columns={"Doctor Name": "Physician Full Name"}, inplace=True)

if "ASM" not in ilms_df.columns:
    ilms_df["ASM"] = None
if "Region" not in ilms_df.columns:
    ilms_df["Region"] = None

df = df.merge(
    ilms_df[["Physician Full Name", "ASM", "Region"]],
    on="Physician Full Name",
    how="left",
    suffixes=("", "_ILMS")
)

df["ASM"] = df["ASM"].fillna(df["ASM_ILMS"])
df["Region"] = df["Region"].fillna(df["Region_ILMS"])
df.drop(columns=["ASM_ILMS", "Region_ILMS"], inplace=True)

df["Order Date V2"] = pd.to_datetime(df["Order Created Date"], dayfirst=True, errors="coerce")
df["Accession Timestamp V2"] = pd.to_datetime(df["Accession Timestamp"], dayfirst=True, errors="coerce")
df["Sample Collection Timestamp V2"] = pd.to_datetime(df["Sample Collection TimeStamp"], dayfirst=True, errors="coerce")
df["Accession Status Clean"] = df["Accession Status"].astype(str).str.strip().str.title()
df["Business Clean"] = df["Business"].astype(str).str.strip().str.title()

cleaned_df = df[
    (df["Order Date V2"] <= cutoff_date) &
    (df["Business Clean"] == "Service") &
    (df["Payment Type"].isin(valid_payment_types))
].drop_duplicates()

accessioned_df = cleaned_df[
    (cleaned_df["Accession Status Clean"] == "Accessioned") &
    (cleaned_df["Order Date V2"].dt.month == month_input) &
    (cleaned_df["Order Date V2"].dt.year == year_input) &
    (cleaned_df["Order Date V2"] <= cutoff_date)
].copy()

accessioned_df["Final Date"] = accessioned_df["Accession Timestamp V2"]
accessioned_df["Final Date"] = accessioned_df["Final Date"].fillna(accessioned_df["Order Date V2"])

today = datetime.now()
start_of_month = datetime(year_input, month_input, 1)
yesterday_end = today.replace(hour=23, minute=59, second=59, microsecond=0) - pd.Timedelta(days=1)

ordered_df = cleaned_df[
    (cleaned_df["Accession Status Clean"].isin(["Ordered", "Collected"])) &
    (df["Order Date V2"] >= start_of_month) &
    (df["Order Date V2"] <= yesterday_end)
].drop_duplicates()

common_patients = set(accessioned_df["Patient Name"]) & set(ordered_df["Patient Name"])
matches = []
for name in common_patients:
    acc_rows = accessioned_df[accessioned_df["Patient Name"] == name][
        ["Patient Name", "Test Ordered", "Total Payable Amount"]
    ]
    ord_rows = ordered_df[ordered_df["Patient Name"] == name][
        ["Patient Name", "Test Ordered", "Total Payable Amount"]
    ]
    combined = pd.concat([acc_rows, ord_rows], ignore_index=True)
    matches.append(combined)

if matches:
    results_df = pd.concat(matches).drop_duplicates()
    print("\n👥 Patients found in both ACCESSIONED & ORDERED (by Patient Name only):")
    print(results_df.to_string(index=False))
else:
    print("\n✅ No matching patients found between ACCESSIONED & ORDERED.")

ordered_dupes = (
    ordered_df.groupby("Patient Name")
    .filter(lambda x: len(x) > 1)
    [["Patient Name", "Test Ordered", "Total Payable Amount"]]
    .drop_duplicates()
)

if not ordered_dupes.empty:
    print("\n👥 Patients appearing more than once in ORDERED (by Patient Name only):")
    print(ordered_dupes.to_string(index=False))
else:
    print("\n✅ No duplicate Patient Name found in ORDERED.")

accessioned_dupes = (
    accessioned_df.groupby("Patient Name")
    .filter(lambda x: len(x) > 1)
    [["Patient Name", "Test Ordered", "Total Payable Amount"]]
    .drop_duplicates()
)

if not accessioned_dupes.empty:
    print("\n👥 Patients appearing more than once in ACCESSIONED (by Patient Name only):")
    print(accessioned_dupes.to_string(index=False))
else:
    print("\n✅ No duplicate Patient Name found in ACCESSIONED.")

cancelled_patients = []
cancelled_in_ordered = ordered_df[
    ordered_df["Patient Name"].isin(cancelled_patients)
][["Patient Name", "Total Payable Amount"]].drop_duplicates()

if not cancelled_in_ordered.empty:
    print("\n🚫 Patients whose orders were manually cancelled:")
    print(cancelled_in_ordered.to_string(index=False))
else:
    print("\n✅ No manually cancelled patients found in ORDERED.")

this_month_df = cleaned_df[
    (cleaned_df["Order Date V2"].dt.month == month_input) &
    (cleaned_df["Order Date V2"].dt.year == year_input)
]
prev_months_df = cleaned_df[
    ((cleaned_df["Order Date V2"].dt.month < month_input) |
     (df["Order Date V2"].dt.year < year_input))
]

repeat_patients = pd.merge(
    this_month_df[["Patient Name"]],
    prev_months_df[["Patient Name", "Total Payable Amount"]],
    on="Patient Name",
    how="inner"
).drop_duplicates()

if not repeat_patients.empty:
    print("\n🔷 Patients found in current and previous months (from Cleaned Sheet):")
    print(repeat_patients.to_string(index=False))
else:
    print("\n✅ No repeat patients from previous months found in Cleaned Sheet.")

if not repeat_patients.empty:
    repeat_in_accessioned = accessioned_df[
        accessioned_df["Patient Name"].isin(repeat_patients["Patient Name"])
    ][["Patient Name", "Total Payable Amount"]].drop_duplicates()

    repeat_in_ordered = ordered_df[
        ordered_df["Patient Name"].isin(repeat_patients["Patient Name"])
    ][["Patient Name", "Total Payable Amount"]].drop_duplicates()

    if not repeat_in_accessioned.empty:
        print("\n📋 These repeat patients are also present in ACCESSIONED:")
        print(repeat_in_accessioned.to_string(index=False))
    else:
        print("\n✅ No repeat patients from previous months found in ACCESSIONED.")

    if not repeat_in_ordered.empty:
        print("\n📋 These repeat patients are also present in ORDERED:")
        print(repeat_in_ordered.to_string(index=False))
    else:
        print("\n✅ No repeat patients from previous months found in ORDERED.")

problem_case_df = cleaned_df[
    (cleaned_df["Accession Status Clean"] == "Problem Case") &
    (df["Order Date V2"].dt.month == month_input) &
    (df["Order Date V2"].dt.year == year_input) &
    (df["Order Date V2"] <= cutoff_date)
].drop_duplicates()

onhold_df = cleaned_df[
    (cleaned_df["Accession Status Clean"] == "On-Hold") &
    (df["Order Date V2"].dt.month == month_input) &
    (df["Order Date V2"].dt.year == year_input) &
    (df["Order Date V2"] <= cutoff_date)
].drop_duplicates()

accessioned_total = accessioned_df['Total Payable Amount'].sum()
ordered_total = ordered_df['Total Payable Amount'].sum()
matched_total = results_df['Total Payable Amount'].sum() if 'results_df' in locals() and not results_df.empty else 0
cancelled_total = cancelled_in_ordered['Total Payable Amount'].sum() if not cancelled_in_ordered.empty else 0
dupes_total = ordered_dupes['Total Payable Amount'].sum() if not ordered_dupes.empty else 0
deductions = matched_total + cancelled_total + dupes_total
adjusted_ordered_total = ordered_total - deductions

print("\n📋 Summary:")
print(f"   Accessioned Total         : ₹{accessioned_total:,.2f}")
print(f"   Ordered Total (original)  : ₹{ordered_total:,.2f}")
print(f"   - Matched Patients         : ₹{matched_total:,.2f}")
print(f"   - Cancelled Patients       : ₹{cancelled_total:,.2f}")
print(f"   - Duplicate Patient Name   : ₹{dupes_total:,.2f}")
print(f"   ---------------------------------------------")
print(f"✅ Ordered Total (adjusted)   : ₹{adjusted_ordered_total:,.2f}")

from pandas import ExcelWriter

def format_dates(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%d-%b-%Y")
    return df

ordered_final = format_dates(ordered_df[[
    "Order Date V2", "Accession Status", "Order Number", "Patient Name",
    "Physician Full Name", "Facility/Hospital Name", "ASM",
    "Test Ordered", "Total Payable Amount", "Payment Type"
]], ["Order Date V2"])

accessioned_final = format_dates(accessioned_df[[
    "Final Date", "Accession Status", "Order Number", "Patient Name",
    "Physician Full Name", "Facility/Hospital Name", "ASM",
    "Test Ordered", "Total Payable Amount", "Payment Type"
]].rename(columns={"Final Date": "Accession Date"}), ["Accession Date"])

problem_case_final = format_dates(problem_case_df[[
    "Order Date V2", "Accession Status", "Order Number", "Patient Name",
    "Physician Full Name", "Facility/Hospital Name", "ASM",
    "Test Ordered", "Total Payable Amount", "Payment Type"
]], ["Order Date V2"])

onhold_final = format_dates(onhold_df[[
    "Order Date V2", "Accession Status", "Order Number", "Patient Name",
    "Physician Full Name", "Facility/Hospital Name", "ASM",
    "Test Ordered", "Total Payable Amount", "Payment Type"
]], ["Order Date V2"])

cleaned_full_df = format_dates(df.copy(), [
    "Order Date V2", "Accession Timestamp V2", "Sample Collection Timestamp V2"
])

excel_output_path = "output/I-LIMS_Cleaned_Ordered_Accessioned_Dec2025.xlsx"

with ExcelWriter(excel_output_path, engine='xlsxwriter') as writer:
    accessioned_final.to_excel(writer, index=False, sheet_name="Accessioned")
    ordered_final.to_excel(writer, index=False, sheet_name="Ordered")
    if not problem_case_final.empty:
        problem_case_final.to_excel(writer, index=False, sheet_name="Problem Case")
    if not onhold_final.empty:
        onhold_final.to_excel(writer, index=False, sheet_name="On-Hold")
    raw_dump_df.to_excel(writer, index=False, sheet_name="Raw Dump")
    cleaned_full_df.to_excel(writer, index=False, sheet_name="Cleaned Sheet")
    if not cancelled_in_ordered.empty:
        cancelled_in_ordered.to_excel(writer, index=False, sheet_name="Cancelled")

print("✅ Output Excel created at:", excel_output_path)
