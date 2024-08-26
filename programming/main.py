class Merge:
  def __init__(self) -> None:
    self.__header: list[str] = []
    self.__data: list[dict[str, str]] = []
  
  def read_csv(self, path_in_1: str, path_in_2: str) -> None:
    with open(path_in_1, 'r') as file_1, open(path_in_2, 'r') as file_2:
      reader_1: list[str] = file_1.read().split('\n')
      header_1: list[str] = reader_1[0].split(',')
      data_1: list[str] = reader_1[1:-1]
      
      reader_2: list[str] = file_2.read().split('\n')
      header_2: list[str] = reader_2[0].split(',')
      data_2: list[str] = reader_2[1:-1]
      
      self.__header: list[str] = sorted(list(set(header_1 + header_2)))
      
      for row_1 in data_1:
        self.__data.append(dict(zip(header_1, row_1.split(','))))
      
      for row_2 in data_2:
        self.__data.append(dict(zip(header_2, row_2.split(','))))
  
  def write_csv(self, path_out: str) -> None:
    with open(path_out, 'w') as file:
      file.write(','.join(self.__header) + '\n')
      
      for row in self.__data:
        file.write(','.join([self.__fill_in(row, column) for column in self.__header]) + '\n')
  
  def __fill_in(self, row: dict[str, str], column: str) -> str:
    if ((column == "{timezone}") and (row.get(column, '') == '')):
      return "UTC+0000"
    elif ((column == "{created_at}") and (row.get(column, '') == '')):
      return '0'
    elif ((column in ["{os_name}", "{device_type}", "{store}", "{platform}"]) and (row.get(column, '') == '')):
      return "unknown"
    else:
      return row.get(column, '')

if __name__ == "__main__":
  path_in_1: str = "C:/Disk/interview/indiez/datasets/test_1.csv"
  path_in_2: str = "C:/Disk/interview/indiez/datasets/test_2.csv"
  path_out: str = "C:/Disk/interview/indiez/programming/output.csv"

  merge: Merge = Merge()
  merge.read_csv(path_in_1, path_in_2)
  merge.write_csv(path_out)
