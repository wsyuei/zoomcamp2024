import pyarrow as pa

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
   
    #    passenger count is greater than 0 
    df = data[data['passenger_count'] > 0]

    #    the trip distance is greater than zero
    df = df[df['trip_distance'] > 0]

    #   list column and 4 columns are in camelCase
    # for col in df.columns:
    #     print(col)

    #   convert datetime to date
    df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date

    #convert camelCase to snake_case
    df.columns = (df.columns.str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True).str.lower())
    
    #Find out values existing in vendor_id, 1 and 2
    # df_list_unique =set(df['vendor_id'])

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
