Feature: Direct Mapping of AE High Level Term

Scenario: Positive Flow
Given the mapping configuration is in the following table
    |  Source Table  | Source Variable  | Target Table | Target Variable | Mapping    |
    |           AE   |     USUBJID		|     AE       |    USUBJID      | Direct Map |
    |           AE   |     AEHLTTM      |     AE       |    AEHLT        | Direct Map |

And an AE table 
   | USUBJID  | AEHLTTM     |
   | 0010001  | INSOMNIA    |
   | 0010002  | DROWSINESS  |

When the configuration is run
Then the AE output table should contain
   | USUBJID  | AEHLT      |
   | 0010001  | INSOMNIA   |
   | 0010002  | DROWSINESS |
