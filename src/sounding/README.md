- **_retrieve_as.py_**: Retrieves atmospheric sounding data. 

##### Script **_gen_sounding_indices.py_** 

This script will generate atmospheric instability indices for the data retrieveed by the script **_retrieve_as.py_**. Data from the SBGL sounding (located at the Galeão Airport, Rio de Janeiro - Brazil) will be used to calculate atmospheric instability indices, generating a new dataset. This new dataset contains one entry per sounding probe. SBGL sounding station produces two probes per day (at 00:00h and 12:00h UTC). Each entry in the produced contains the values of the computed instability indices for one probe. The following instability indices are computed:

- CAPE
- CIN
- Lift
- k
- Total totals
- Show alter
