import pandas as pd
import warnings
from datetime import datetime

# ----------------------------
# Warning handling (VS Code safe)
# ----------------------------
try:
    from pandas.errors import SettingWithCopyWarning
    warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
except Exception:
    warnings.simplefilter(action="ignore")

# ----------------------------
# Step 0: Dynamic input
# ----------------------------
month_input = 12
year_input = 2025
cutoff_date = datetime(year_input, month_input, 9, 23, 59, 59)

# ----------------------------
# Step 1: Load main dataset
# ----------------------------
dump_path = "data/daily.csv"
df = pd.read_csv(dump_path)
raw_dump_df = df.copy()

# ----------------------------
# Step 2: Create BUSINESS column
# ----------------------------
df["Business"] = ""

intl_countries = [
    "Egypt", "Turkey", "Nepal", "Malyasia", "Uzbekistan", "Malaysia",
    "Jordan", "Türkiye", "TÃ¼rkiye", "EGYPT", "EGPYT", "UAE"
]

df.loc[(df["Business"] == "") & df["Country"].isin(intl_countries), "Business"] = "International"
df.loc[(df["Business"] == "") & (df["Facility/Hospital Name"] == "Cancer institute W.I.A"), "Business"] = "Non-Service"
df.loc[(df["Business"] == "") & (df["Order Created By"] == "indx2.bot@indx.ai"), "Business"] = "Non-Service"

foc_users = [
    "sharmada.wagle@onecelldx.com", "priti.thate@onecelldx.com",
    "rajarshi.bhattacharjee@onecelldx.com", "navya.nandiraju@onecelldx.com",
    "snehal.kathwate@onecelldx.com"
]

df.loc[
    (df["Business"] == "") &
    (df["Payment Status"] == "FOC") &
    df["Order Created By"].isin(foc_users),
    "Business"
] = "Non-Service"

df.loc[
    (df["Business"] == "") &
    (df["Payment Status"] == "FOC") &
    (~df["Order Created By"].isin(foc_users)) &
    (df["Order Created By"] != "indx2.bot@indx.ai"),
    "Business"
] = "Service FOC"

df.loc[
    (df["Business"] == "") &
    df["Sample Category"].astype(str).str.contains("Service", case=False, na=False),
    "Business"
] = "Service"

df.loc[df["Business"] == "", "Business"] = "Non-Service"

# ----------------------------
# Step 3: PAYMENT TYPE
# ----------------------------
df["Payment Type"] = ""
df.loc[df["Order Type"] == "MOU", "Payment Type"] = "B2B"
df.loc[df["Order Type"] == "Retail", "Payment Type"] = "B2C"
df.loc[df["Order Type"] == "FOC", "Payment Type"] = "FOC"
df.loc[df["Payment Status"] == "FOC", "Payment Type"] = "FOC"
df.loc[df["Order Type"].astype(str).str.contains("Research", na=False), "Payment Type"] = "Other"

valid_payment_types = ["B2B", "B2C", "Other"]

# ----------------------------
# Step 4: Merge ASM + REGION (Email Grouping)
# ----------------------------
asm_df = pd.read_excel("data/email grouping updated.xlsx")

# ⬇️ FIX: force column names to string before .str
asm_df.columns = asm_df.columns.map(str).str.strip()

asm_df.rename(
    columns={"Email - Id": "Order Created By", "ASM NAME": "ASM", "Region": "Region"},
    inplace=True
)

asm_map = asm_df.drop_duplicates("Order Created By").set_index("Order Created By")[["ASM", "Region"]]
df = df.merge(asm_map, on="Order Created By", how="left")

# ----------------------------
# Step 4.1: ILIMS grouping fallback
# ----------------------------
ilms_df = pd.read_excel("data/ilims data grouping (3).xlsx")

# ⬇️ FIX: force column names to string before .str
ilms_df.columns = ilms_df.columns.map(str).str.strip()

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

# ----------------------------
# Step 5: Dates & cleaning
# ----------------------------
df["Order Date V2"] = pd.to_datetime(df["Order Created Date"], dayfirst=True, errors="coerce")
df["Accession Timestamp V2"] = pd.to_datetime(df["Accession Timestamp"], dayfirst=True, errors="coerce")
df["Sample Collection Timestamp V2"] = pd.to_datetime(df["Sample Collection TimeStamp"], dayfirst=True, errors="coerce")

df["Accession Status Clean"] = df["Accession Status"].astype(str).str.strip().str.title()
df["Business Clean"] = df["Business"].astype(str).str.strip().str.title()

# ----------------------------
# Step 6: Cleaned Sheet
# ----------------------------
cleaned_df = df[
    (df["Order Date V2"] <= cutoff_date) &
    (df["Business Clean"] == "Service") &
    (df["Payment Type"].isin(valid_payment_types))
].drop_duplicates()

# ----------------------------
# Step 7: Accessioned
# ----------------------------
accessioned_df = cleaned_df[
    (cleaned_df["Accession Status Clean"] == "Accessioned") &
    (cleaned_df["Order Date V2"].dt.month == month_input) &
    (cleaned_df["Order Date V2"].dt.year == year_input) &
    (cleaned_df["Order Date V2"] <= cutoff_date)
].copy()

accessioned_df["Final Date"] = accessioned_df["Accession Timestamp V2"].fillna(
    accessioned_df["Order Date V2"]
)

# ----------------------------
# Step 7.1: Ordered
# ----------------------------
today = datetime.now()
start_of_month = datetime(year_input, month_input, 1)
yesterday_end = today.replace(hour=23, minute=59, second=59, microsecond=0) - pd.Timedelta(days=1)

ordered_df = cleaned_df[
    (cleaned_df["Accession Status Clean"].isin(["Ordered", "Collected"])) &
    (cleaned_df["Order Date V2"] >= start_of_month) &
    (cleaned_df["Order Date V2"] <= yesterday_end)
].drop_duplicates()

# ----------------------------
# Step 8–12 logic (unchanged)
# ----------------------------
common_patients = set(accessioned_df["Patient Name"]) & set(ordered_df["Patient Name"])
matches = []

for name in common_patients:
    acc_rows = accessioned_df[accessioned_df["Patient Name"] == name][
        ["Patient Name", "Test Ordered", "Total Payable Amount"]
    ]
    ord_rows = ordered_df[ordered_df["Patient Name"] == name][
        ["Patient Name", "Test Ordered", "Total Payable Amount"]
    ]
    matches.append(pd.concat([acc_rows, ord_rows]))

results_df = pd.concat(matches).drop_duplicates() if matches else pd.DataFrame()

ordered_dupes = (
    ordered_df.groupby("Patient Name")
    .filter(lambda x: len(x) > 1)[["Patient Name", "Test Ordered", "Total Payable Amount"]]
    .drop_duplicates()
)

accessioned_dupes = (
    accessioned_df.groupby("Patient Name")
    .filter(lambda x: len(x) > 1)[["Patient Name", "Test Ordered", "Total Payable Amount"]]
    .drop_duplicates()
)

cancelled_patients = []
cancelled_in_ordered = ordered_df[
    ordered_df["Patient Name"].isin(cancelled_patients)
][["Patient Name", "Total Payable Amount"]].drop_duplicates()

accessioned_total = accessioned_df["Total Payable Amount"].sum()
ordered_total = ordered_df["Total Payable Amount"].sum()
matched_total = results_df["Total Payable Amount"].sum() if not results_df.empty else 0
cancelled_total = cancelled_in_ordered["Total Payable Amount"].sum() if not cancelled_in_ordered.empty else 0
dupes_total = ordered_dupes["Total Payable Amount"].sum() if not ordered_dupes.empty else 0

adjusted_ordered_total = ordered_total - (matched_total + cancelled_total + dupes_total)

# ----------------------------
# Step 13: Export Excel
# ----------------------------
from pandas import ExcelWriter

def format_dates(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%d-%b-%Y")
    return df

ordered_final = format_dates(ordered_df, ["Order Date V2"])
accessioned_final = format_dates(
    accessioned_df.rename(columns={"Final Date": "Accession Date"}),
    ["Accession Date"]
)
cleaned_full_df = format_dates(
    df.copy(),
    ["Order Date V2", "Accession Timestamp V2", "Sample Collection Timestamp V2"]
)

output_file = "output/I-LIMS_Cleaned_Ordered_Accessioned_Dec2025.xlsx"

with ExcelWriter(output_file, engine="xlsxwriter") as writer:
    accessioned_final.to_excel(writer, index=False, sheet_name="Accessioned")
    ordered_final.to_excel(writer, index=False, sheet_name="Ordered")
    raw_dump_df.to_excel(writer, index=False, sheet_name="Raw Dump")
    cleaned_full_df.to_excel(writer, index=False, sheet_name="Cleaned Sheet")

print("✅ Script completed successfully")
