def get_rules_as_list_of_dict():
  return [
    {
      "name": "valid_id",
      "constraint": "employee_id IS NOT NULL",
      "tag": "validity"
    },
    {
      "name": "valid_email",
      "constraint": "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'",
      "tag": "validity"
    }
  ]