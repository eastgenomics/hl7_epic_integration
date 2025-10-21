import argparse
import socket

from hl7apy.parser import parse_message


def create_test_hl7_message():
    """
    Create an hl7 message as an ACK from the original hl7 message received
    Parameters
    ----------
    original_message : string
        hl7 message received
    Returns
    -------
    str:
        HL7 ACK message in MLLP format
    """

    msg = (
        "MSH|^~\&|Epic|EPIC||TEST_1844|20250501120337|LABBACKGROUND|ORU^R01^ORU_R01|40165|T|2.5.1|||||||||LRI_NG_RN_Profile^Profile Component^2.16.840.1.113883.9.20^ISO\r"
        "PID|1||3111691^^^CUH^MR||BEAKER^ADY||19851122|M|||^^^^CB2 0QQ^^P|||||||||||||||||||N|||20221122134431|1\r"
        "ORC|RE||SP-25120R0004^Beaker|SP-25120R0004^Beaker|CM||^^^^^R|^SP-25120R0004&Beaker|20250430151815|374^CLINICAL SCIENTIST^GLH^^||29489^CONSULTANT^PAS^^^^^^PROVID^^^^PROVID|SPEC:PATH^ADD SPECIALTY PATH LAB^DEPID^ADD^^^^^|||||WLHPC1FR8AH^ADD-CSSD-WS-036||||||ADDENBROOKE'S HOSPITAL^HILLS ROAD^CAMBRIDGE^^CB2 0QQ^^C^^CAMBRIDGESHIRE|||||||LAB9521^RARE DISEASE GENOMIC TESTING LAB ONLY^BEAKER^^^^^^RARE DISEASE GENOMIC TESTING LAB ONLY\r"
        "OBR|1||SP-25120R0004^Beaker|LAB9620^RARE DISEASE NGS ANALYSIS^BEAKER^^^^^^RARE DISEASE NGS ANALYSIS|||0000||||G|||20250430151732||29489^CONSULTANT^PAS^^^^^^PROVID^^^^PROVID||||||20250430161000||Lab|C|4782&ASSAY TEST&LRRBEAKER&&&&&&ASSAY TEST|||^SP-25120R0004&Beaker|||&SCIENTIST&Glh&CLINICAL&||||||||||||||||||LAB9521^RARE DISEASE GENOMIC TESTING LAB ONLY^BEAKER^^^^^^RARE DISEASE GENOMIC TESTING LAB ONLY\r"
        "TQ1|1||||||||R\r"
        "OBX|1|ST|4852^GLH REASON FOR REFERRAL^LRRBEAKER^^^^^^GLH REASON FOR REFERRAL||AVL Testing||||||F|||0000|||||20250430161000||||NHS EAST GENOMIC LABORATORY HUB (699M0)^D^^^^LLB BEAKER^LLB BEAKER^^^184|CAMBRIDGE UNIVERSITY TRUST^ADDENBROOKE'S HOSPITAL^CAMBRIDGE^^CB2 0QQ^ENG^B^^CAMBRIDGESHIRE\r"
        "OBX|2|ST|5067^GLH RESULT SUMMARY^LRRBEAKER^^^^^^GLH RESULT SUMMARY||lalalal||||||F|||0000|||||20250430161000||||NHS EAST GENOMIC LABORATORY HUB (699M0)^D^^^^LLB BEAKER^LLB BEAKER^^^184|CAMBRIDGE UNIVERSITY TRUST^ADDENBROOKE'S HOSPITAL^CAMBRIDGE^^CB2 0QQ^ENG^B^^CAMBRIDGESHIRE\r"
        "OBX|3|ST|5068^GLH RESULT^LRRBEAKER^^^^^^GLH RESULT||fsdfsdfsaf||||||F|||0000|||||20250430161000||||NHS EAST GENOMIC LABORATORY HUB (699M0)^D^^^^LLB BEAKER^LLB BEAKER^^^184|CAMBRIDGE UNIVERSITY TRUST^ADDENBROOKE'S HOSPITAL^CAMBRIDGE^^CB2 0QQ^ENG^B^^CAMBRIDGESHIRE\r"
        "OBX|4|CWE|Genome Assembly|1|5^hg19^^^^^^^^^^^^1.2.840.114350.1.13.366.3.7.4.866582.1040||||||C\r"
        "OBX|5|CWE|Variant Category|2a|1^Simple^^^^^^^^^^^^1.2.840.114350.1.13.366.3.7.4.866582.35||||||C\r"
        "OBX|6|CWE|Discrete Genetic Variant|2a|^123:123||||||C\r"
        "OBX|7|ST|Chromosome|2a|16||||||C\r"
        "OBX|8|CWE|DNA Change Type|2a|2^Deletion^^^^^^^^^^^^1.2.840.114350.1.13.366.3.7.4.866582.1240||||||C\r"
        "OBX|9|CWE|Molecular Consequence|2a|120^Intergenic Variant^^^^^^^^^^^^1.2.840.114350.1.13.366.3.7.4.866582.1260||||||C\r"
        "OBX|10|CWE|Genomic Reference Sequence ID|2a|123^123||||||C\r"
        "OBX|11|CWE|Genomic DNA Change|2a|123^123^HGVS.g||||||C\r"
        "OBX|12|CWE|Genetic Variant Source|2a|1^Germline^^^^^^^^^^^^1.2.840.114350.1.13.366.3.7.4.866582.1650||||||C\r"
        "OBX|13|CWE|Genetic Variant Assessment|2a|1^Detected^^^^^^^^^^^^1.2.840.114350.1.13.366.3.7.4.866582.5000||||||C\r"
        "OBX|14|CWE|Gene Studied|2a|5^A1BG^HGNC||||||C\r"
        "OBX|15|CWE|Variant Classification|2a|4^Likely benign^^^^^^^^^^^^1.2.840.114350.1.13.366.3.7.4.866582.1907||||||C\r"
        "SPM|1|||Fluid^Fluid^^^^^^^Fluid|||||||||||||0000|20250430151732\r"
        "ZSP|1|100062483\r"
    )

    parsed_msg = parse_message(msg)

    return parsed_msg.to_mllp()


def main(host, port):
    hl7_message = create_test_hl7_message()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(hl7_message.encode("utf-8"))
        data = s.recv(1024)
        print(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    args = parser.parse_args()
    main(args.host, args.port)
