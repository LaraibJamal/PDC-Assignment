import pandas as pd
from concurrent.futures import ProcessPoolExecutor

# Load the CSV files
students_data = pd.read_csv('students.csv')
fees_data = pd.read_csv('fees.csv')

# Ensure 'student_id' columns are integers and trim any whitespace
students_data["student_id"] = students_data["student_id"].astype(str).str.strip().astype(int)
fees_data["student_id"] = fees_data["student_id"].astype(str).str.strip().astype(int)

# Print unique student IDs for debugging purposes
print("Unique Student IDs in students_data:", students_data["student_id"].unique())
print("Unique Student IDs in fees_data:", fees_data["student_id"].unique())

# Define a function to identify the most relevant fee submission date for each student
def determine_relevant_date(group):
    date_frequency = group["fee_submission_date"].value_counts()
    if all(date_frequency == 1):  # Check if all dates are distinct
        return group["fee_submission_date"].max()  # Use the most recent date
    return date_frequency.idxmax()  # Use the most frequent date

# Precompute the most relevant fee date for each student
relevant_dates = fees_data.groupby("student_id").apply(determine_relevant_date).reset_index()
relevant_dates.columns = ["student_id", "relevant_date"]

# Function to process individual student records
def process_student(student_row):
    student_id = student_row["student_id"]

    if not pd.isna(student_id):  # Ensure the student_id is valid
        # Retrieve the relevant fee date for the student
        relevant_date_row = relevant_dates[relevant_dates["student_id"] == student_id]

        if not relevant_date_row.empty:
            relevant_date = relevant_date_row["relevant_date"].iloc[0]
            return f"Student ID {student_id}: Relevant fee date: {relevant_date}"
        else:
            return f"Student ID {student_id}: No fee records found."
    return f"Invalid Student ID: {student_id}"

# Main execution block for parallel processing
if __name__ == "__main__":
    # Convert student data into a list of row dictionaries for parallel execution
    student_rows = students_data.to_dict("records")

    # Use ProcessPoolExecutor to parallelize student processing
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(process_student, student_rows))

    # Display results
    for result in results:
        print(result)