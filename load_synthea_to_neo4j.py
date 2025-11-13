"""
Synthea Data to Neo4j Loader
=============================
This script loads all 18 Synthea CSV files into a Neo4j graph database.
It creates nodes for entities and relationships between them based on foreign keys.

Requirements:
    pip install neo4j pandas

Configuration:
    Update NEO4J_URI, NEO4J_USER, and NEO4J_PASSWORD with your credentials
"""

from neo4j import GraphDatabase
import pandas as pd
import os
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Neo4j Configuration
NEO4J_URI = "bolt://localhost:7687"  # Update with your Neo4j URI
NEO4J_USER = "neo4j"                 # Update with your username
NEO4J_PASSWORD = "Shubham@1997"          # Update with your password

# CSV Files Directory
CSV_DIR = r"D:\shubham\LLM & Neo4j\synthea\sample_data"  # Update with your CSV directory path


class SyntheaToNeo4jLoader:
    """Load Synthea CSV data into Neo4j graph database"""
    
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.batch_size = 1000
        
    def close(self):
        self.driver.close()
    
    def clear_database(self):
        """Clear all nodes and relationships from the database"""
        with self.driver.session() as session:
            logger.info("Clearing database...")
            session.run("MATCH (n) DETACH DELETE n")
            logger.info("Database cleared")
    
    def create_constraints(self):
        """Create unique constraints for better performance and data integrity"""
        with self.driver.session() as session:
            constraints = [
                "CREATE CONSTRAINT patient_id IF NOT EXISTS FOR (p:Patient) REQUIRE p.id IS UNIQUE",
                "CREATE CONSTRAINT encounter_id IF NOT EXISTS FOR (e:Encounter) REQUIRE e.id IS UNIQUE",
                "CREATE CONSTRAINT organization_id IF NOT EXISTS FOR (o:Organization) REQUIRE o.id IS UNIQUE",
                "CREATE CONSTRAINT provider_id IF NOT EXISTS FOR (pr:Provider) REQUIRE pr.id IS UNIQUE",
                "CREATE CONSTRAINT payer_id IF NOT EXISTS FOR (py:Payer) REQUIRE py.id IS UNIQUE",
                "CREATE CONSTRAINT careplan_id IF NOT EXISTS FOR (cp:CarePlan) REQUIRE cp.id IS UNIQUE",
                "CREATE CONSTRAINT claim_id IF NOT EXISTS FOR (cl:Claim) REQUIRE cl.id IS UNIQUE",
                "CREATE CONSTRAINT transaction_id IF NOT EXISTS FOR (ct:ClaimTransaction) REQUIRE ct.id IS UNIQUE"
            ]
            
            for constraint in constraints:
                try:
                    session.run(constraint)
                    logger.info(f"Created constraint: {constraint.split('FOR')[1].split('REQUIRE')[0].strip()}")
                except Exception as e:
                    logger.warning(f"Constraint already exists or error: {e}")
    
    def create_indexes(self):
        """Create indexes for better query performance"""
        with self.driver.session() as session:
            indexes = [
                "CREATE INDEX patient_ssn IF NOT EXISTS FOR (p:Patient) ON (p.ssn)",
                "CREATE INDEX encounter_date IF NOT EXISTS FOR (e:Encounter) ON (e.start)",
                "CREATE INDEX condition_code IF NOT EXISTS FOR (c:Condition) ON (c.code)",
                "CREATE INDEX medication_code IF NOT EXISTS FOR (m:Medication) ON (m.code)",
                "CREATE INDEX procedure_code IF NOT EXISTS FOR (pr:Procedure) ON (pr.code)"
            ]
            
            for index in indexes:
                try:
                    session.run(index)
                    logger.info(f"Created index: {index}")
                except Exception as e:
                    logger.warning(f"Index already exists or error: {e}")
    
    def load_patients(self, csv_path):
        """Load Patient nodes"""
        logger.info("Loading Patients...")
        df = pd.read_csv(csv_path)
        
        cypher = """
        UNWIND $rows AS row
        CREATE (p:Patient {
            id: row.Id,
            birthDate: row.BIRTHDATE,
            deathDate: row.DEATHDATE,
            ssn: row.SSN,
            drivers: row.DRIVERS,
            passport: row.PASSPORT,
            prefix: row.PREFIX,
            firstName: row.FIRST,
            middleName: row.MIDDLE,
            lastName: row.LAST,
            suffix: row.SUFFIX,
            maiden: row.MAIDEN,
            marital: row.MARITAL,
            race: row.RACE,
            ethnicity: row.ETHNICITY,
            gender: row.GENDER,
            birthPlace: row.BIRTHPLACE,
            address: row.ADDRESS,
            city: row.CITY,
            state: row.STATE,
            county: row.COUNTY,
            fips: row.FIPS,
            zip: row.ZIP,
            lat: toFloat(row.LAT),
            lon: toFloat(row.LON),
            healthcareExpenses: toFloat(row.HEALTHCARE_EXPENSES),
            healthcareCoverage: toFloat(row.HEALTHCARE_COVERAGE),
            income: toFloat(row.INCOME)
        })
        """
        
        self._load_in_batches(df, cypher)
        logger.info(f"Loaded {len(df)} patients")
    
    def load_organizations(self, csv_path):
        """Load Organization nodes"""
        logger.info("Loading Organizations...")
        df = pd.read_csv(csv_path)
        
        cypher = """
        UNWIND $rows AS row
        CREATE (o:Organization {
            id: row.Id,
            name: row.NAME,
            address: row.ADDRESS,
            city: row.CITY,
            state: row.STATE,
            zip: row.ZIP,
            lat: toFloat(row.LAT),
            lon: toFloat(row.LON),
            phone: row.PHONE,
            revenue: toFloat(row.REVENUE),
            utilization: toInteger(row.UTILIZATION)
        })
        """
        
        self._load_in_batches(df, cypher)
        logger.info(f"Loaded {len(df)} organizations")
    
    def load_providers(self, csv_path):
        """Load Provider nodes and relationships to Organizations"""
        logger.info("Loading Providers...")
        df = pd.read_csv(csv_path)
        
        cypher = """
        UNWIND $rows AS row
        CREATE (pr:Provider {
            id: row.Id,
            organizationId: row.ORGANIZATION,
            name: row.NAME,
            gender: row.GENDER,
            speciality: row.SPECIALITY,
            address: row.ADDRESS,
            city: row.CITY,
            state: row.STATE,
            zip: row.ZIP,
            lat: toFloat(row.LAT),
            lon: toFloat(row.LON),
            encounters: toInteger(row.ENCOUNTERS),
            procedures: toInteger(row.PROCEDURES)
        })
        """
        
        self._load_in_batches(df, cypher)
        
        # Create relationships to Organizations
        rel_cypher = """
        MATCH (pr:Provider)
        MATCH (o:Organization {id: pr.organizationId})
        MERGE (pr)-[:EMPLOYED_BY]->(o)
        """
        
        with self.driver.session() as session:
            session.run(rel_cypher)
        
        logger.info(f"Loaded {len(df)} providers")
    
    def load_payers(self, csv_path):
        """Load Payer nodes"""
        logger.info("Loading Payers...")
        df = pd.read_csv(csv_path)
        
        cypher = """
        UNWIND $rows AS row
        CREATE (py:Payer {
            id: row.Id,
            name: row.NAME,
            ownership: row.OWNERSHIP,
            address: row.ADDRESS,
            city: row.CITY,
            stateHeadquartered: row.STATE_HEADQUARTERED,
            zip: row.ZIP,
            phone: row.PHONE,
            amountCovered: toFloat(row.AMOUNT_COVERED),
            amountUncovered: toFloat(row.AMOUNT_UNCOVERED),
            revenue: toFloat(row.REVENUE),
            coveredEncounters: toInteger(row.COVERED_ENCOUNTERS),
            uncoveredEncounters: toInteger(row.UNCOVERED_ENCOUNTERS),
            coveredMedications: toInteger(row.COVERED_MEDICATIONS),
            uncoveredMedications: toInteger(row.UNCOVERED_MEDICATIONS),
            coveredProcedures: toInteger(row.COVERED_PROCEDURES),
            uncoveredProcedures: toInteger(row.UNCOVERED_PROCEDURES),
            coveredImmunizations: toInteger(row.COVERED_IMMUNIZATIONS),
            uncoveredImmunizations: toInteger(row.UNCOVERED_IMMUNIZATIONS),
            uniqueCustomers: toInteger(row.UNIQUE_CUSTOMERS),
            qolsAvg: toFloat(row.QOLS_AVG),
            memberMonths: toInteger(row.MEMBER_MONTHS)
        })
        """
        
        self._load_in_batches(df, cypher)
        logger.info(f"Loaded {len(df)} payers")
    
    def load_encounters(self, csv_path):
        """Load Encounter nodes and relationships"""
        logger.info("Loading Encounters...")
        df = pd.read_csv(csv_path)
        
        cypher = """
        UNWIND $rows AS row
        CREATE (e:Encounter {
            id: row.Id,
            start: row.START,
            stop: row.STOP,
            patientId: row.PATIENT,
            organizationId: row.ORGANIZATION,
            providerId: row.PROVIDER,
            payerId: row.PAYER,
            encounterClass: row.ENCOUNTERCLASS,
            code: row.CODE,
            description: row.DESCRIPTION,
            baseCost: toFloat(row.BASE_ENCOUNTER_COST),
            totalClaimCost: toFloat(row.TOTAL_CLAIM_COST),
            payerCoverage: toFloat(row.PAYER_COVERAGE),
            reasonCode: row.REASONCODE,
            reasonDescription: row.REASONDESCRIPTION
        })
        """
        
        self._load_in_batches(df, cypher)
        
        # Create relationships
        logger.info("Creating Encounter relationships...")
        relationships = [
            "MATCH (e:Encounter) MATCH (p:Patient {id: e.patientId}) MERGE (p)-[:HAD_ENCOUNTER]->(e)",
            "MATCH (e:Encounter) MATCH (o:Organization {id: e.organizationId}) MERGE (e)-[:OCCURRED_AT]->(o)",
            "MATCH (e:Encounter) MATCH (pr:Provider {id: e.providerId}) MERGE (e)-[:ATTENDED_BY]->(pr)",
            "MATCH (e:Encounter) MATCH (py:Payer {id: e.payerId}) MERGE (e)-[:COVERED_BY]->(py)"
        ]
        
        with self.driver.session() as session:
            for rel in relationships:
                session.run(rel)
        
        logger.info(f"Loaded {len(df)} encounters")
    
    def load_conditions(self, csv_path):
        """Load Condition nodes and relationships"""
        logger.info("Loading Conditions...")
        df = pd.read_csv(csv_path)
        
        cypher = """
        UNWIND $rows AS row
        CREATE (c:Condition {
            start: row.START,
            stop: row.STOP,
            patientId: row.PATIENT,
            encounterId: row.ENCOUNTER,
            system: row.SYSTEM,
            code: row.CODE,
            description: row.DESCRIPTION
        })
        """
        
        self._load_in_batches(df, cypher)
        
        # Create relationships
        logger.info("Creating Condition relationships...")
        with self.driver.session() as session:
            session.run("""
                MATCH (c:Condition)
                MATCH (p:Patient {id: c.patientId})
                MERGE (p)-[:HAS_CONDITION]->(c)
            """)
            session.run("""
                MATCH (c:Condition)
                MATCH (e:Encounter {id: c.encounterId})
                MERGE (e)-[:DIAGNOSED]->(c)
            """)
        
        logger.info(f"Loaded {len(df)} conditions")
    
    def load_medications(self, csv_path):
        """Load Medication nodes and relationships"""
        logger.info("Loading Medications...")
        df = pd.read_csv(csv_path)
        
        cypher = """
        UNWIND $rows AS row
        CREATE (m:Medication {
            start: row.START,
            stop: row.STOP,
            patientId: row.PATIENT,
            payerId: row.PAYER,
            encounterId: row.ENCOUNTER,
            code: row.CODE,
            description: row.DESCRIPTION,
            baseCost: toFloat(row.BASE_COST),
            payerCoverage: toFloat(row.PAYER_COVERAGE),
            dispenses: toInteger(row.DISPENSES),
            totalCost: toFloat(row.TOTALCOST),
            reasonCode: row.REASONCODE,
            reasonDescription: row.REASONDESCRIPTION
        })
        """
        
        self._load_in_batches(df, cypher)
        
        # Create relationships
        logger.info("Creating Medication relationships...")
        with self.driver.session() as session:
            session.run("""
                MATCH (m:Medication)
                MATCH (p:Patient {id: m.patientId})
                MERGE (p)-[:PRESCRIBED]->(m)
            """)
            session.run("""
                MATCH (m:Medication)
                MATCH (e:Encounter {id: m.encounterId})
                MERGE (e)-[:PRESCRIBED_MEDICATION]->(m)
            """)
            session.run("""
                MATCH (m:Medication)
                WHERE m.payerId IS NOT NULL
                MATCH (py:Payer {id: m.payerId})
                MERGE (m)-[:PAID_BY]->(py)
            """)
        
        logger.info(f"Loaded {len(df)} medications")
    
    def load_procedures(self, csv_path):
        """Load Procedure nodes and relationships"""
        logger.info("Loading Procedures...")
        df = pd.read_csv(csv_path)
        
        cypher = """
        UNWIND $rows AS row
        CREATE (pr:Procedure {
            start: row.START,
            stop: row.STOP,
            patientId: row.PATIENT,
            encounterId: row.ENCOUNTER,
            system: row.SYSTEM,
            code: row.CODE,
            description: row.DESCRIPTION,
            baseCost: toFloat(row.BASE_COST),
            reasonCode: row.REASONCODE,
            reasonDescription: row.REASONDESCRIPTION
        })
        """
        
        self._load_in_batches(df, cypher)
        
        # Create relationships
        logger.info("Creating Procedure relationships...")
        with self.driver.session() as session:
            session.run("""
                MATCH (pr:Procedure)
                MATCH (p:Patient {id: pr.patientId})
                MERGE (p)-[:UNDERWENT_PROCEDURE]->(pr)
            """)
            session.run("""
                MATCH (pr:Procedure)
                MATCH (e:Encounter {id: pr.encounterId})
                MERGE (e)-[:PERFORMED_PROCEDURE]->(pr)
            """)
        
        logger.info(f"Loaded {len(df)} procedures")
    
    def load_immunizations(self, csv_path):
        """Load Immunization nodes and relationships"""
        logger.info("Loading Immunizations...")
        df = pd.read_csv(csv_path)
        
        cypher = """
        UNWIND $rows AS row
        CREATE (i:Immunization {
            date: row.DATE,
            patientId: row.PATIENT,
            encounterId: row.ENCOUNTER,
            code: row.CODE,
            description: row.DESCRIPTION,
            cost: toFloat(row.COST)
        })
        """
        
        self._load_in_batches(df, cypher)
        
        # Create relationships
        logger.info("Creating Immunization relationships...")
        with self.driver.session() as session:
            session.run("""
                MATCH (i:Immunization)
                MATCH (p:Patient {id: i.patientId})
                MERGE (p)-[:RECEIVED_IMMUNIZATION]->(i)
            """)
            session.run("""
                MATCH (i:Immunization)
                MATCH (e:Encounter {id: i.encounterId})
                MERGE (e)-[:ADMINISTERED_IMMUNIZATION]->(i)
            """)
        
        logger.info(f"Loaded {len(df)} immunizations")
    
    def load_observations(self, csv_path):
        """Load Observation nodes and relationships"""
        logger.info("Loading Observations...")
        df = pd.read_csv(csv_path)
        
        cypher = """
        UNWIND $rows AS row
        CREATE (o:Observation {
            date: row.DATE,
            patientId: row.PATIENT,
            encounterId: row.ENCOUNTER,
            category: row.CATEGORY,
            code: row.CODE,
            description: row.DESCRIPTION,
            value: row.VALUE,
            units: row.UNITS,
            type: row.TYPE
        })
        """
        
        self._load_in_batches(df, cypher)
        
        # Create relationships
        logger.info("Creating Observation relationships...")
        with self.driver.session() as session:
            session.run("""
                MATCH (o:Observation)
                MATCH (p:Patient {id: o.patientId})
                MERGE (p)-[:HAS_OBSERVATION]->(o)
            """)
            session.run("""
                MATCH (o:Observation)
                MATCH (e:Encounter {id: o.encounterId})
                MERGE (e)-[:RECORDED_OBSERVATION]->(o)
            """)
        
        logger.info(f"Loaded {len(df)} observations")
    
    def load_allergies(self, csv_path):
        """Load Allergy nodes and relationships"""
        logger.info("Loading Allergies...")
        df = pd.read_csv(csv_path)
        
        cypher = """
        UNWIND $rows AS row
        CREATE (a:Allergy {
            start: row.START,
            stop: row.STOP,
            patientId: row.PATIENT,
            encounterId: row.ENCOUNTER,
            code: row.CODE,
            system: row.SYSTEM,
            description: row.DESCRIPTION,
            type: row.TYPE,
            category: row.CATEGORY,
            reaction1: row.REACTION1,
            description1: row.DESCRIPTION1,
            severity1: row.SEVERITY1,
            reaction2: row.REACTION2,
            description2: row.DESCRIPTION2,
            severity2: row.SEVERITY2
        })
        """
        
        self._load_in_batches(df, cypher)
        
        # Create relationships
        logger.info("Creating Allergy relationships...")
        with self.driver.session() as session:
            session.run("""
                MATCH (a:Allergy)
                MATCH (p:Patient {id: a.patientId})
                MERGE (p)-[:HAS_ALLERGY]->(a)
            """)
            session.run("""
                MATCH (a:Allergy)
                MATCH (e:Encounter {id: a.encounterId})
                MERGE (e)-[:DOCUMENTED_ALLERGY]->(a)
            """)
        
        logger.info(f"Loaded {len(df)} allergies")
    
    def load_careplans(self, csv_path):
        """Load CarePlan nodes and relationships"""
        logger.info("Loading Care Plans...")
        df = pd.read_csv(csv_path)
        
        cypher = """
        UNWIND $rows AS row
        CREATE (cp:CarePlan {
            id: row.Id,
            start: row.START,
            stop: row.STOP,
            patientId: row.PATIENT,
            encounterId: row.ENCOUNTER,
            code: row.CODE,
            description: row.DESCRIPTION,
            reasonCode: row.REASONCODE,
            reasonDescription: row.REASONDESCRIPTION
        })
        """
        
        self._load_in_batches(df, cypher)
        
        # Create relationships
        logger.info("Creating CarePlan relationships...")
        with self.driver.session() as session:
            session.run("""
                MATCH (cp:CarePlan)
                MATCH (p:Patient {id: cp.patientId})
                MERGE (p)-[:HAS_CAREPLAN]->(cp)
            """)
            session.run("""
                MATCH (cp:CarePlan)
                MATCH (e:Encounter {id: cp.encounterId})
                MERGE (e)-[:INITIATED_CAREPLAN]->(cp)
            """)
        
        logger.info(f"Loaded {len(df)} careplans")
    
    def load_devices(self, csv_path):
        """Load Device nodes and relationships"""
        logger.info("Loading Devices...")
        df = pd.read_csv(csv_path)
        
        cypher = """
        UNWIND $rows AS row
        CREATE (d:Device {
            start: row.START,
            stop: row.STOP,
            patientId: row.PATIENT,
            encounterId: row.ENCOUNTER,
            code: row.CODE,
            description: row.DESCRIPTION,
            udi: row.UDI
        })
        """
        
        self._load_in_batches(df, cypher)
        
        # Create relationships
        logger.info("Creating Device relationships...")
        with self.driver.session() as session:
            session.run("""
                MATCH (d:Device)
                MATCH (p:Patient {id: d.patientId})
                MERGE (p)-[:USES_DEVICE]->(d)
            """)
            session.run("""
                MATCH (d:Device)
                MATCH (e:Encounter {id: d.encounterId})
                MERGE (e)-[:ASSOCIATED_DEVICE]->(d)
            """)
        
        logger.info(f"Loaded {len(df)} devices")
    
    def load_imaging_studies(self, csv_path):
        """Load ImagingStudy nodes and relationships"""
        logger.info("Loading Imaging Studies...")
        df = pd.read_csv(csv_path)
        
        cypher = """
        UNWIND $rows AS row
        CREATE (img:ImagingStudy {
            id: row.Id,
            date: row.DATE,
            patientId: row.PATIENT,
            encounterId: row.ENCOUNTER,
            seriesUid: row.SERIES_UID,
            bodySiteCode: row.BODYSITE_CODE,
            bodySiteDescription: row.BODYSITE_DESCRIPTION,
            modalityCode: row.MODALITY_CODE,
            modalityDescription: row.MODALITY_DESCRIPTION,
            instanceUid: row.INSTANCE_UID,
            sopCode: row.SOP_CODE,
            sopDescription: row.SOP_DESCRIPTION,
            procedureCode: row.PROCEDURE_CODE
        })
        """
        
        self._load_in_batches(df, cypher)
        
        # Create relationships
        logger.info("Creating ImagingStudy relationships...")
        with self.driver.session() as session:
            session.run("""
                MATCH (img:ImagingStudy)
                MATCH (p:Patient {id: img.patientId})
                MERGE (p)-[:HAD_IMAGING]->(img)
            """)
            session.run("""
                MATCH (img:ImagingStudy)
                MATCH (e:Encounter {id: img.encounterId})
                MERGE (e)-[:CONDUCTED_IMAGING]->(img)
            """)
        
        logger.info(f"Loaded {len(df)} imaging studies")
    
    def load_supplies(self, csv_path):
        """Load Supply nodes and relationships"""
        logger.info("Loading Supplies...")
        df = pd.read_csv(csv_path)
        
        cypher = """
        UNWIND $rows AS row
        CREATE (s:Supply {
            date: row.DATE,
            patientId: row.PATIENT,
            encounterId: row.ENCOUNTER,
            code: row.CODE,
            description: row.DESCRIPTION,
            quantity: toInteger(row.QUANTITY)
        })
        """
        
        self._load_in_batches(df, cypher)
        
        # Create relationships
        logger.info("Creating Supply relationships...")
        with self.driver.session() as session:
            session.run("""
                MATCH (s:Supply)
                MATCH (p:Patient {id: s.patientId})
                MERGE (p)-[:USED_SUPPLY]->(s)
            """)
            session.run("""
                MATCH (s:Supply)
                MATCH (e:Encounter {id: s.encounterId})
                MERGE (e)-[:CONSUMED_SUPPLY]->(s)
            """)
        
        logger.info(f"Loaded {len(df)} supplies")
    
    def load_payer_transitions(self, csv_path):
        """Load PayerTransition relationships"""
        logger.info("Loading Payer Transitions...")
        df = pd.read_csv(csv_path)
        
        cypher = """
        UNWIND $rows AS row
        CREATE (pt:PayerTransition {
            patientId: row.PATIENT,
            memberId: row.MEMBERID,
            startYear: row.START_YEAR,
            endYear: row.END_YEAR,
            payerId: row.PAYER,
            secondaryPayerId: row.SECONDARY_PAYER,
            ownership: row.OWNERSHIP,
            ownerName: row.OWNERNAME
        })
        """
        
        self._load_in_batches(df, cypher)
        
        # Create relationships
        logger.info("Creating PayerTransition relationships...")
        with self.driver.session() as session:
            session.run("""
                MATCH (pt:PayerTransition)
                MATCH (p:Patient {id: pt.patientId})
                MERGE (p)-[:HAD_COVERAGE]->(pt)
            """)
            session.run("""
                MATCH (pt:PayerTransition)
                MATCH (py:Payer {id: pt.payerId})
                MERGE (pt)-[:PRIMARY_PAYER]->(py)
            """)
            session.run("""
                MATCH (pt:PayerTransition)
                WHERE pt.secondaryPayerId IS NOT NULL
                MATCH (py:Payer {id: pt.secondaryPayerId})
                MERGE (pt)-[:SECONDARY_PAYER]->(py)
            """)
        
        logger.info(f"Loaded {len(df)} payer transitions")
    
    def load_claims(self, csv_path):
        """Load Claim nodes and relationships"""
        logger.info("Loading Claims...")
        df = pd.read_csv(csv_path)
        
        cypher = """
        UNWIND $rows AS row
        CREATE (cl:Claim {
            id: row.Id,
            patientId: row.PATIENTID,
            providerId: row.PROVIDERID,
            primaryInsuranceId: row.PRIMARYPATIENTINSURANCEID,
            secondaryInsuranceId: row.SECONDARYPATIENTINSURANCEID,
            departmentId: toInteger(row.DEPARTMENTID),
            patientDepartmentId: toInteger(row.PATIENTDEPARTMENTID),
            diagnosis1: row.DIAGNOSIS1,
            diagnosis2: row.DIAGNOSIS2,
            diagnosis3: row.DIAGNOSIS3,
            diagnosis4: row.DIAGNOSIS4,
            diagnosis5: row.DIAGNOSIS5,
            diagnosis6: row.DIAGNOSIS6,
            diagnosis7: row.DIAGNOSIS7,
            diagnosis8: row.DIAGNOSIS8,
            referringProviderId: row.REFERRINGPROVIDERID,
            appointmentId: row.APPOINTMENTID,
            currentIllnessDate: row.CURRENTILLNESSDATE,
            serviceDate: row.SERVICEDATE,
            supervisingProviderId: row.SUPERVISINGPROVIDERID,
            status1: row.STATUS1,
            status2: row.STATUS2,
            statusP: row.STATUSP,
            outstanding1: toFloat(row.OUTSTANDING1),
            outstanding2: toFloat(row.OUTSTANDING2),
            outstandingP: toFloat(row.OUTSTANDINGP),
            lastBilledDate1: row.LASTBILLEDDATE1,
            lastBilledDate2: row.LASTBILLEDDATE2,
            lastBilledDateP: row.LASTBILLEDDATEP,
            healthcareClaimTypeId1: toInteger(row.HEALTHCARECLAIMTYPEID1),
            healthcareClaimTypeId2: toInteger(row.HEALTHCARECLAIMTYPEID2)
        })
        """
        
        self._load_in_batches(df, cypher)
        
        # Create relationships
        logger.info("Creating Claim relationships...")
        with self.driver.session() as session:
            session.run("""
                MATCH (cl:Claim)
                MATCH (p:Patient {id: cl.patientId})
                MERGE (p)-[:FILED_CLAIM]->(cl)
            """)
            session.run("""
                MATCH (cl:Claim)
                MATCH (pr:Provider {id: cl.providerId})
                MERGE (cl)-[:SUBMITTED_BY]->(pr)
            """)
            session.run("""
                MATCH (cl:Claim)
                WHERE cl.primaryInsuranceId IS NOT NULL
                MATCH (py:Payer {id: cl.primaryInsuranceId})
                MERGE (cl)-[:PRIMARY_INSURANCE]->(py)
            """)
            session.run("""
                MATCH (cl:Claim)
                WHERE cl.secondaryInsuranceId IS NOT NULL
                MATCH (py:Payer {id: cl.secondaryInsuranceId})
                MERGE (cl)-[:SECONDARY_INSURANCE]->(py)
            """)
            session.run("""
                MATCH (cl:Claim)
                WHERE cl.appointmentId IS NOT NULL
                MATCH (e:Encounter {id: cl.appointmentId})
                MERGE (cl)-[:FOR_ENCOUNTER]->(e)
            """)
        
        logger.info(f"Loaded {len(df)} claims")
    
    def load_claims_transactions(self, csv_path):
        """Load ClaimTransaction nodes and relationships"""
        logger.info("Loading Claims Transactions...")
        df = pd.read_csv(csv_path)
        
        cypher = """
        UNWIND $rows AS row
        CREATE (ct:ClaimTransaction {
            id: row.Id,
            claimId: row.CLAIMID,
            chargeId: toInteger(row.CHARGEID),
            patientId: row.PATIENTID,
            type: row.TYPE,
            amount: toFloat(row.AMOUNT),
            method: row.METHOD,
            fromDate: row.FROMDATE,
            toDate: row.TODATE,
            placeOfService: row.PLACEOFSERVICE,
            procedureCode: row.PROCEDURECODE,
            modifier1: row.MODIFIER1,
            modifier2: row.MODIFIER2,
            diagnosisRef1: toInteger(row.DIAGNOSISREF1),
            diagnosisRef2: toInteger(row.DIAGNOSISREF2),
            diagnosisRef3: toInteger(row.DIAGNOSISREF3),
            diagnosisRef4: toInteger(row.DIAGNOSISREF4),
            units: toInteger(row.UNITS),
            departmentId: toInteger(row.DEPARTMENTID),
            notes: row.NOTES,
            unitAmount: toFloat(row.UNITAMOUNT),
            transferOutId: toInteger(row.TRANSFEROUTID),
            transferType: row.TRANSFERTYPE,
            payments: toFloat(row.PAYMENTS),
            adjustments: toFloat(row.ADJUSTMENTS),
            transfers: toFloat(row.TRANSFERS),
            outstanding: toFloat(row.OUTSTANDING),
            appointmentId: row.APPOINTMENTID,
            lineNote: row.LINENOTE,
            patientInsuranceId: row.PATIENTINSURANCEID,
            feeScheduleId: toInteger(row.FEEscheduleid),
            providerId: row.PROVIDERID,
            supervisingProviderId: row.SUPERVISINGPROVIDERID
        })
        """
        
        self._load_in_batches(df, cypher)
        
        # Create relationships
        logger.info("Creating ClaimTransaction relationships...")
        with self.driver.session() as session:
            session.run("""
                MATCH (ct:ClaimTransaction)
                MATCH (cl:Claim {id: ct.claimId})
                MERGE (cl)-[:HAS_TRANSACTION]->(ct)
            """)
            session.run("""
                MATCH (ct:ClaimTransaction)
                MATCH (p:Patient {id: ct.patientId})
                MERGE (ct)-[:FOR_PATIENT]->(p)
            """)
            session.run("""
                MATCH (ct:ClaimTransaction)
                WHERE ct.placeOfService IS NOT NULL
                MATCH (o:Organization {id: ct.placeOfService})
                MERGE (ct)-[:SERVICE_AT]->(o)
            """)
            session.run("""
                MATCH (ct:ClaimTransaction)
                WHERE ct.providerId IS NOT NULL
                MATCH (pr:Provider {id: ct.providerId})
                MERGE (ct)-[:PERFORMED_BY]->(pr)
            """)
            session.run("""
                MATCH (ct:ClaimTransaction)
                WHERE ct.appointmentId IS NOT NULL
                MATCH (e:Encounter {id: ct.appointmentId})
                MERGE (ct)-[:DURING_ENCOUNTER]->(e)
            """)
        
        logger.info(f"Loaded {len(df)} claims transactions")
    
    def _load_in_batches(self, df, cypher):
        """Helper method to load data in batches"""
        df = df.fillna('')  # Replace NaN with empty string
        records = df.to_dict('records')
        
        with self.driver.session() as session:
            for i in range(0, len(records), self.batch_size):
                batch = records[i:i + self.batch_size]
                session.run(cypher, rows=batch)
    
    def load_all_data(self, csv_dir):
        """Load all Synthea CSV files into Neo4j"""
        start_time = datetime.now()
        logger.info("=" * 80)
        logger.info("Starting Synthea to Neo4j data load")
        logger.info("=" * 80)
        
        # Create constraints and indexes
        self.create_constraints()
        self.create_indexes()
        
        # Load core entity tables first
        logger.info("\n--- Loading Core Entities ---")
        self.load_patients(os.path.join(csv_dir, "patients.csv"))
        self.load_organizations(os.path.join(csv_dir, "organizations.csv"))
        self.load_providers(os.path.join(csv_dir, "providers.csv"))
        self.load_payers(os.path.join(csv_dir, "payers.csv"))
        
        # Load encounters (central to most relationships)
        logger.info("\n--- Loading Encounters ---")
        self.load_encounters(os.path.join(csv_dir, "encounters.csv"))
        
        # Load clinical data
        logger.info("\n--- Loading Clinical Data ---")
        self.load_conditions(os.path.join(csv_dir, "conditions.csv"))
        self.load_medications(os.path.join(csv_dir, "medications.csv"))
        self.load_procedures(os.path.join(csv_dir, "procedures.csv"))
        self.load_immunizations(os.path.join(csv_dir, "immunizations.csv"))
        self.load_observations(os.path.join(csv_dir, "observations.csv"))
        self.load_allergies(os.path.join(csv_dir, "allergies.csv"))
        self.load_careplans(os.path.join(csv_dir, "careplans.csv"))
        self.load_devices(os.path.join(csv_dir, "devices.csv"))
        self.load_imaging_studies(os.path.join(csv_dir, "imaging_studies.csv"))
        self.load_supplies(os.path.join(csv_dir, "supplies.csv"))
        
        # Load insurance and claims data
        logger.info("\n--- Loading Insurance & Claims Data ---")
        self.load_payer_transitions(os.path.join(csv_dir, "payer_transitions.csv"))
        self.load_claims(os.path.join(csv_dir, "claims.csv"))
        self.load_claims_transactions(os.path.join(csv_dir, "claims_transactions.csv"))
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("=" * 80)
        logger.info(f"Data load completed in {duration:.2f} seconds")
        logger.info("=" * 80)
        
        # Print statistics
        self.print_statistics()
    
    def print_statistics(self):
        """Print database statistics"""
        with self.driver.session() as session:
            logger.info("\n--- Database Statistics ---")
            
            # Count nodes by label
            result = session.run("""
                MATCH (n)
                RETURN labels(n)[0] as label, count(*) as count
                ORDER BY count DESC
            """)
            
            logger.info("\nNode Counts:")
            for record in result:
                logger.info(f"  {record['label']}: {record['count']}")
            
            # Count relationships by type
            result = session.run("""
                MATCH ()-[r]->()
                RETURN type(r) as type, count(*) as count
                ORDER BY count DESC
            """)
            
            logger.info("\nRelationship Counts:")
            for record in result:
                logger.info(f"  {record['type']}: {record['count']}")


def main():
    """Main execution function"""
    # Initialize loader
    loader = SyntheaToNeo4jLoader(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    try:
        # Optional: Clear existing data
        # Uncomment the line below if you want to clear the database first
        # loader.clear_database()
        
        # Load all data
        loader.load_all_data(CSV_DIR)
        
    except Exception as e:
        logger.error(f"Error during data load: {e}", exc_info=True)
    finally:
        loader.close()
        logger.info("Neo4j connection closed")


if __name__ == "__main__":
    main()
