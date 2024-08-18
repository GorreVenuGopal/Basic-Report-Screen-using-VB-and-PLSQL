-- Create or replace the package in a new schema "HR_DEPT"
CREATE OR REPLACE PACKAGE HR_DEPT.EMPLOYEE_DATA
AS
   -- Define a REF CURSOR type to be used for returning query results
   TYPE emp_cur IS REF CURSOR;

   -- Procedure to retrieve employee details based on specific criteria
   PROCEDURE get_employee_details (
      p_start_date   IN       VARCHAR2, -- Start date filter
      p_end_date     IN       VARCHAR2, -- End date filter
      p_employee_id  IN       VARCHAR2, -- Employee ID filter
      p_cur          OUT      emp_cur   -- Cursor to return the result set
   );

   -- Procedure to modify employee records
   PROCEDURE modify_employee_record (
      p_employee_id  IN VARCHAR2, -- Employee ID to be modified
      p_rtn_msg      OUT VARCHAR2 -- Return message indicating success or error
   );

   -- Function to convert and retrieve job title based on the latest revision
   FUNCTION get_latest_job_title (
      input_job_code IN VARCHAR2 -- Input job code to be converted
   ) RETURN VARCHAR2;
END EMPLOYEE_DATA;

/

----------------------------------------------------------

-- Package body implementation
CREATE OR REPLACE PACKAGE BODY HR_DEPT.EMPLOYEE_DATA
IS
   -- Implementation of the get_employee_details procedure
   PROCEDURE get_employee_details (
      p_start_date   IN       VARCHAR2,
      p_end_date     IN       VARCHAR2,
      p_employee_id  IN       VARCHAR2,
      p_cur          OUT      emp_cur
   )
   AS
      v_count NUMBER;
   BEGIN
      BEGIN
         -- Counting total employees in the temp_employee table (can be used to check for data existence)
         SELECT COUNT(*)
           INTO v_count
           FROM temp_employees;
      EXCEPTION
         -- Handle any unexpected errors during the count operation
         WHEN OTHERS THEN
            INSERT INTO error_logs
            VALUES ('EMPLOYEE_DATA Error: ' || DBMS_UTILITY.format_error_backtrace);
            RAISE;
      END;

      -- If count is greater than 0, return employees from temp_employees based on the date range and employee ID
      IF v_count > 0 THEN
         OPEN p_cur FOR
         SELECT *
           FROM (SELECT e.emp_id AS employee_id, 
                        e.first_name AS first_name, 
                        e.last_name AS last_name, 
                        1 AS no_of_records,
                        e.salary AS salary,
                        get_latest_job_title(e.job_code) AS job_title,
                        j.department_id AS department_id,
                        j.manager_id AS manager_id,
                        TO_DATE(e.hire_date, 'DDMMYYYYHH24MISS') AS hire_date
                   FROM employees e
                   LEFT JOIN job_history j ON e.emp_id = j.emp_id
                   JOIN (SELECT j.job_code,
                                MAX(j.revision_no) AS max_revision_no
                             FROM job_history j
                            GROUP BY j.job_code) mj
                   ON e.job_code = mj.job_code AND j.revision_no = mj.max_revision_no
                   ) sub
          WHERE sub.hire_date BETWEEN TO_DATE(p_start_date, 'DDMMYYYYHH24MISS')
                                AND TO_DATE(p_end_date, 'DDMMYYYYHH24MISS')
            AND sub.employee_id IN (SELECT emp_id FROM temp_employees);

      ELSE
         -- If count is zero, return employees from the employees table based on the date range and employee ID
         OPEN p_cur FOR
         SELECT *
           FROM (SELECT e.emp_id AS employee_id, 
                        e.first_name AS first_name, 
                        e.last_name AS last_name, 
                        1 AS no_of_records,
                        e.salary AS salary,
                        get_latest_job_title(e.job_code) AS job_title,
                        j.department_id AS department_id,
                        j.manager_id AS manager_id,
                        TO_DATE(e.hire_date, 'DDMMYYYYHH24MISS') AS hire_date
                   FROM employees e
                   LEFT JOIN job_history j ON e.emp_id = j.emp_id
                   JOIN (SELECT j.job_code,
                                MAX(j.revision_no) AS max_revision_no
                             FROM job_history j
                            GROUP BY j.job_code) mj
                   ON e.job_code = mj.job_code AND j.revision_no = mj.max_revision_no
                   ) sub
          WHERE sub.hire_date BETWEEN TO_DATE(p_start_date, 'DDMMYYYYHH24MISS')
                                AND TO_DATE(p_end_date, 'DDMMYYYYHH24MISS')
            AND sub.employee_id LIKE '%' || p_employee_id || '%';

      END IF;

   EXCEPTION
      -- Handle any unexpected errors during the procedure execution
      WHEN OTHERS THEN
         INSERT INTO error_logs
              VALUES ('EMPLOYEE_DATA Exception: ' || DBMS_UTILITY.format_error_backtrace());
         RAISE;
   END get_employee_details;

   -- Implementation of the modify_employee_record procedure
   PROCEDURE modify_employee_record (
      p_employee_id  IN VARCHAR2, 
      p_rtn_msg      OUT VARCHAR2
   )
   AS
   BEGIN
      -- Insert the employee ID into the temp_employees table
      INSERT INTO temp_employees (emp_id)
           VALUES (p_employee_id);

      -- Commit the transaction to save the changes
      COMMIT;
   END modify_employee_record;

   -- Implementation of the get_latest_job_title function
   FUNCTION get_latest_job_title (
      input_job_code IN VARCHAR2
   ) RETURN VARCHAR2
   IS
      converted_job_title VARCHAR2(50);
   BEGIN
      -- Select the most recent revision for the given job code and compute the converted job title
      SELECT TRIM(SUBSTR(j.job_title, 1, 20))
      INTO converted_job_title
      FROM HR_DEPT.JOB_INFO j
      JOIN HR_DEPT.JOB_REVISION r
      ON j.job_code = r.job_code AND j.revision_no = r.revision_no
      WHERE j.job_code = input_job_code
      AND r.revision_no = (
          SELECT MAX(revision_no)
          FROM HR_DEPT.JOB_REVISION
          WHERE job_code = input_job_code
      );

      RETURN converted_job_title;
   EXCEPTION
      WHEN NO_DATA_FOUND THEN
         RETURN NULL; -- Return NULL if no data is found
      WHEN OTHERS THEN
         RAISE; -- Rethrow other exceptions
   END get_latest_job_title;

END EMPLOYEE_DATA;

/
