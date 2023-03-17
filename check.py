from pyspark.sql.functions import udf
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

# here we are combining both udf and directly calculating delta
def calculate_checksum(upc):
    if not isinstance(upc, str) or not upc.isdigit():
        return None  # Invalid input

    
    digits = list(map(int, upc[::-1]))
    odd_sum = F.sum(digits[0::2])  
    even_sum = F.sum(digits[1::2]) 
    total_sum = odd_sum * 3 + even_sum  
    checksum = (10 - (total_sum % 10)) % 10 
    return checksum

calculate_checksum_udf = udf(calculate_checksum, IntegerType())
