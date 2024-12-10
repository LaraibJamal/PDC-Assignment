import pandas as pd

# Load the CSV files into dataframes
students_data = pd.read_csv('students.csv')
fees_data = pd.read_csv('fees.csv')

# Convert the 'student_id' columns to integers after trimming any extra spaces
students_data["student_id"] = students_data["student_id"].astype(str).str.strip().astype(int)
fees_data["student_id"] = fees_data["student_id"].astype(str).str.strip().astype(int)

# Print unique student IDs for verification purposes
print("Student IDs in students_data:", students_data["student_id"].unique())
print("Student IDs in fees_data:", fees_data["student_id"].unique())

# Define a function to identify the most relevant payment date for each student
def determine_relevant_date(group):
    date_frequency = group["fee_submission_date"].value_counts()
    if all(date_frequency == 1):  # Check if all payment dates are distinct
        return group["fee_submission_date"].max()  # Use the most recent date
    return date_frequency.idxmax()  # Use the most frequent date

# Generate a mapping of each student_id to their most relevant fee submission date
relevant_dates = fees_data.groupby("student_id").apply(determine_relevant_date).reset_index()
relevant_dates.columns = ["student_id", "relevant_date"]

# Iterate through each student record to match with the relevant date
for _, student_row in students_data.iterrows():
    student_id = student_row["student_id"]

    if not pd.isna(student_id):  # Confirm that the student_id is valid
        print(f"\nAnalyzing Student ID: {student_id}")
        student_fee_data = relevant_dates[relevant_dates["student_id"] == student_id]

        if not student_fee_data.empty:
            relevant_date = student_fee_data["relevant_date"].iloc[0]
            print(f"Relevant payment date for Student ID {student_id}: {relevant_date}")
        else:
            print(f"No fee data found for Student ID {student_id}")
    else:
        print(f"Invalid entry for Student ID: {student_id}")