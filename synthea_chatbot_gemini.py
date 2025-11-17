"""
Synthea Neo4j Chatbot with LangChain and Google Gemini
=======================================================
This chatbot allows you to query the Synthea Neo4j database using natural language.
It uses LangChain with Google Gemini API to convert natural language questions into Cypher queries.

Requirements:
    pip install langchain langchain-google-genai neo4j python-dotenv google-generativeai

Setup:
    1. Create a .env file with:
       GOOGLE_API_KEY=your_gemini_api_key_here
       NEO4J_URI=bolt://localhost:7687
       NEO4J_USER=neo4j
       NEO4J_PASSWORD=your_password
    
    2. Ensure your Neo4j database is running with Synthea data loaded

Usage:
    python synthea_chatbot_gemini.py
"""

import os
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_community.graphs import Neo4jGraph
from langchain_community.chains.graph_qa.cypher import GraphCypherQAChain
from langchain_core.prompts import PromptTemplate
import sys

# Load environment variables
load_dotenv()

# Configuration
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Validate configuration
if not GOOGLE_API_KEY:
    print("ERROR: GOOGLE_API_KEY not found in environment variables!")
    print("Please create a .env file with your Google Gemini API key.")
    print("\nGet your free API key from: https://makersuite.google.com/app/apikey")
    sys.exit(1)

if not NEO4J_PASSWORD:
    print("ERROR: NEO4J_PASSWORD not found in environment variables!")
    print("Please add your Neo4j password to the .env file.")
    sys.exit(1)


class SyntheaChatbot:
    """
    Chatbot for querying Synthea Neo4j database using natural language with Google Gemini
    """
    
    def __init__(self):
        """Initialize the chatbot with Neo4j connection and Gemini LLM"""
        print("🚀 Initializing Synthea Healthcare Chatbot with Google Gemini...")
        
        # Initialize Neo4j Graph
        try:
            self.graph = Neo4jGraph(
                url=NEO4J_URI,
                username=NEO4J_USER,
                password=NEO4J_PASSWORD
            )
            print("✅ Connected to Neo4j database")
        except Exception as e:
            print(f"❌ Failed to connect to Neo4j: {e}")
            sys.exit(1)
        
        # Initialize Google Gemini LLM
        try:
            self.llm = ChatGoogleGenerativeAI(
                model="gemini-2.5-flash",  # Best model for complex reasoning
                google_api_key=GOOGLE_API_KEY,
                temperature=0,  # Deterministic responses
                convert_system_message_to_human=True  # Gemini compatibility
            )
            print("✅ Connected to Google Gemini API")
            print("📊 Using Model: Gemini 2.5 flash (Best for complex queries)")
        except Exception as e:
            print(f"❌ Failed to initialize Google Gemini: {e}")
            print("\nMake sure you have a valid API key from:")
            print("https://makersuite.google.com/app/apikey")
            sys.exit(1)
        
        # Get database schema
        self.schema = self.graph.get_schema
        print("✅ Retrieved database schema")

        # Create custom prompt templates for Synthea healthcare domain
        self.cypher_prompt = self._create_cypher_prompt()
        self.qa_prompt = self._create_qa_prompt()  # NEW: Add QA prompt
        
        # Create the QA chain
        try:
            self.chain = GraphCypherQAChain.from_llm(
                llm=self.llm,
                graph=self.graph,
                verbose=True,
                return_intermediate_steps=True,
                cypher_prompt=self.cypher_prompt,
                qa_prompt=self.qa_prompt,  # NEW: Use custom QA prompt
                allow_dangerous_requests=True  # Required for executing Cypher queries
            )
            print("✅ Chatbot initialized successfully!\n")
        except Exception as e:
            print(f"❌ Failed to create QA chain: {e}")
            sys.exit(1)
    
    def _create_cypher_prompt(self):
        """Create a custom prompt template optimized for Synthea healthcare data with Gemini"""
        
        template = """You are an expert Neo4j Cypher query generator specialized in healthcare data analysis.
Your task is to convert natural language questions into precise Cypher queries for a Synthea healthcare database.

Database Schema:
{schema}

=== IMPORTANT DATABASE STRUCTURE ===

NODE TYPES (18 total):
1. Patient - Patient demographics and information
   Properties: id, firstName, lastName, birthDate, deathDate, gender, race, ethnicity, ssn, address, city, state, zip, healthcareExpenses, healthcareCoverage, income
   
2. Encounter - Medical visits and appointments
   Properties: id, start, stop, encounterClass, code, description, baseCost, totalClaimCost, payerCoverage, reasonCode
   
3. Condition - Diagnoses and medical conditions
   Properties: code, description, start, stop, system
   
4. Medication - Prescribed medications
   Properties: code, description, start, stop, baseCost, totalCost, payerCoverage, dispenses, reasonCode
   
5. Procedure - Medical procedures and surgeries
   Properties: code, description, start, stop, baseCost, reasonCode, system
   
6. Immunization - Vaccinations
   Properties: code, description, date, cost
   
7. Observation - Lab results and vital signs
   Properties: code, description, date, value, units, type, category
   
8. Allergy - Allergic reactions
   Properties: code, description, start, stop, system, type, category, severity1
   
9. Provider - Healthcare providers and clinicians
   Properties: id, name, gender, speciality, address, city, state, encounters, procedures
   
10. Organization - Healthcare facilities
    Properties: id, name, address, city, state, zip, phone, revenue, utilization
    
11. Payer - Insurance companies
    Properties: id, name, ownership, amountCovered, amountUncovered, revenue
    
12. CarePlan - Treatment plans
    Properties: id, code, description, start, stop, reasonCode

13-18. Device, ImagingStudy, Supply, PayerTransition, Claim, ClaimTransaction

KEY RELATIONSHIPS:
- (Patient)-[:HAD_ENCOUNTER]->(Encounter)
- (Patient)-[:HAS_CONDITION]->(Condition)
- (Patient)-[:PRESCRIBED]->(Medication)
- (Patient)-[:UNDERWENT_PROCEDURE]->(Procedure)
- (Patient)-[:RECEIVED_IMMUNIZATION]->(Immunization)
- (Patient)-[:HAS_OBSERVATION]->(Observation)
- (Patient)-[:HAS_ALLERGY]->(Allergy)
- (Encounter)-[:DIAGNOSED]->(Condition)
- (Encounter)-[:PRESCRIBED_MEDICATION]->(Medication)
- (Encounter)-[:PERFORMED_PROCEDURE]->(Procedure)
- (Encounter)-[:ATTENDED_BY]->(Provider)
- (Encounter)-[:OCCURRED_AT]->(Organization)
- (Encounter)-[:COVERED_BY]->(Payer)
- (Provider)-[:EMPLOYED_BY]->(Organization)

=== CRITICAL CYPHER RULES ===

1. PROPERTY NAMES (Case-Sensitive):
   ✓ firstName, lastName (NOT first_name, last_name)
   ✓ birthDate, deathDate (NOT birth_date, death_date)
   ✓ encounterClass (NOT encounter_class)
   ✓ baseCost, totalCost, totalClaimCost (NOT base_cost, total_cost)
   ✓ healthcareExpenses, healthcareCoverage (NOT healthcare_expenses)

2. DATE OPERATIONS:
   - Use date() function: date(p.birthDate)
   - Calculate age: duration.between(date(p.birthDate), date()).years
   - Date comparisons: date(e.start) > date('2020-01-01')
   - Recent dates: date(e.start) > date() - duration({{days: 30}})

3. TEXT MATCHING:
   - Case-insensitive: toLower(p.lastName) = toLower('smith')
   - Partial match: c.description CONTAINS 'Diabetes'
   - Exact match: p.gender = 'M' or p.gender = 'F'

4. AGGREGATIONS:
   - Count: count(DISTINCT p) or count(*)
   - Sum: sum(e.totalClaimCost)
   - Average: avg(e.baseCost)
   - Always use DISTINCT for patient counts

5. PERFORMANCE:
   - ALWAYS use LIMIT (default: 10) unless user asks for "all"
   - Use WHERE clauses early in the query
   - Check for NULL: WHERE property IS NOT NULL

6. COMMON PATTERNS:
   - Find patients: MATCH (p:Patient) WHERE ... RETURN ...
   - With conditions: MATCH (p:Patient)-[:HAS_CONDITION]->(c:Condition)
   - With encounters: MATCH (p:Patient)-[:HAD_ENCOUNTER]->(e:Encounter)
   - Count by type: RETURN x.property, count(*) ORDER BY count(*) DESC

=== EXAMPLE QUERIES ===

Q: How many patients?
A: MATCH (p:Patient) RETURN count(p) as patientCount

Q: Show diabetic patients
A: MATCH (p:Patient)-[:HAS_CONDITION]->(c:Condition)
   WHERE c.description CONTAINS 'Diabetes'
   RETURN p.firstName, p.lastName, p.gender, 
          duration.between(date(p.birthDate), date()).years as age
   LIMIT 10

Q: Most common conditions
A: MATCH (c:Condition)
   RETURN c.description as condition, count(*) as frequency
   ORDER BY frequency DESC
   LIMIT 10

Q: Average encounter cost
A: MATCH (e:Encounter)
   RETURN avg(e.totalClaimCost) as avgCost, count(e) as totalEncounters

Q: Patients over 65
A: MATCH (p:Patient)
   WHERE duration.between(date(p.birthDate), date()).years >= 65
   RETURN p.firstName, p.lastName, 
          duration.between(date(p.birthDate), date()).years as age
   LIMIT 10

=== YOUR TASK ===

Question: {question}

Generate a single, executable Cypher query that:
1. Answers the question accurately
2. Uses correct property names (case-sensitive)
3. Includes proper date handling if needed
4. Uses LIMIT appropriately (default 10)
5. Returns meaningful column names
6. Handles NULL values if necessary

OUTPUT ONLY THE CYPHER QUERY - NO EXPLANATIONS, NO MARKDOWN, NO CODE BLOCKS.
Just the raw Cypher query that can be executed directly.

Cypher Query:"""
        
        return PromptTemplate(
            template=template,
            input_variables=["schema", "question"]
        )
    
    def _create_qa_prompt(self):
        """Create a prompt template for generating natural language answers from query results"""
        
        template = """You are a helpful healthcare data assistant. Your task is to provide clear, 
natural language answers based on database query results.

You have been asked the following question:
Question: {question}

The database returned these results:
{context}

IMPORTANT INSTRUCTIONS:
1. ALWAYS provide a clear, natural language answer based on the results
2. DO NOT say "I don't know" if results are provided
3. Format the answer in a friendly, readable way
4. Include specific numbers and details from the results
5. If results are a list, format them clearly with numbers or bullets
6. If results contain counts or aggregations, state them clearly
7. Be concise but informative

FORMATTING GUIDELINES:
- For counts: "There are X [items] in the database"
- For lists: Format as numbered list with names and details
- For aggregations: "The average/total is X"
- For empty results: "No [items] were found matching your criteria"

Examples:

Q: "How many patients?"
Results: [{{'count': 124}}]
A: "There are 124 patients in the database."

Q: "Show me diabetic patients"
Results: [{{'firstName': 'John', 'lastName': 'Doe', 'age': 67}}, {{'firstName': 'Jane', 'lastName': 'Smith', 'age': 54}}]
A: "Here are diabetic patients:
1. John Doe (Age: 67)
2. Jane Smith (Age: 54)"

Q: "Most common conditions"
Results: [{{'condition': 'Hypertension', 'count': 45}}, {{'condition': 'Diabetes', 'count': 32}}]
A: "The most common conditions are:
1. Hypertension - 45 patients
2. Diabetes - 32 patients"

Now, answer the question based on the provided results. BE SPECIFIC and USE THE DATA PROVIDED.

Answer:"""
        
        return PromptTemplate(
            template=template,
            input_variables=["question", "context"]
        )
    
    def ask(self, question):
        """
        Ask a question in natural language and get an answer
        
        Args:
            question (str): Natural language question about the healthcare data
            
        Returns:
            dict: Contains 'answer', 'cypher_query', and 'raw_results'
        """
        try:
            # Get response from the chain
            response = self.chain.invoke({"query": question})
            
            # Extract components
            answer = response.get('result', 'No answer generated')
            cypher_query = None
            raw_results = None
            
            if 'intermediate_steps' in response:
                steps = response['intermediate_steps']
                if len(steps) > 0:
                    cypher_query = steps[0].get('query', None)
                if len(steps) > 1:
                    raw_results = steps[1].get('context', None)
            
            return {
                'answer': answer,
                'cypher_query': cypher_query,
                'raw_results': raw_results
            }
            
        except Exception as e:
            return {
                'answer': f"Error processing question: {str(e)}",
                'cypher_query': None,
                'raw_results': None
            }
    
    def get_database_stats(self):
        """Get basic statistics about the database"""
        query = """
        MATCH (n)
        RETURN labels(n)[0] as NodeType, count(*) as Count
        ORDER BY Count DESC
        """
        result = self.graph.query(query)
        return result
    
    def get_sample_patients(self, limit=5):
        """Get sample patient information"""
        query = f"""
        MATCH (p:Patient)
        RETURN p.firstName as FirstName, 
               p.lastName as LastName, 
               p.gender as Gender,
               p.birthDate as BirthDate
        LIMIT {limit}
        """
        result = self.graph.query(query)
        return result


def print_banner():
    """Print welcome banner"""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║        🏥 SYNTHEA HEALTHCARE CHATBOT 🏥                      ║
║                                                              ║
║        Powered by Google Gemini 1.5 Pro + Neo4j             ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝

Ask me anything about the patient healthcare data!

Example questions:
• How many patients are in the database?
• Show me patients with diabetes
• What are the most common conditions?
• Which medications are prescribed most often?
• Find patients with high blood pressure
• What's the average cost of encounters?
• List all providers and their specialties
• Show me recent emergency visits
• Find patients over 65 years old
• Calculate total healthcare costs

Type 'help' for more commands, 'exit' to quit.
"""
    print(banner)


def print_help():
    """Print help information"""
    help_text = """
Available Commands:
-------------------
help        - Show this help message
exit/quit   - Exit the chatbot
stats       - Show database statistics
samples     - Show sample patients
clear       - Clear the screen
schema      - Show database schema
models      - Show available Gemini models

Example Questions:
------------------
Patient Queries:
• How many patients are in the database?
• Show me male patients over 65
• Find patients born in Massachusetts
• List patients with the last name Smith
• Show me all deceased patients

Condition Queries:
• What are the most common conditions?
• How many patients have diabetes?
• Show me all chronic conditions
• Find patients with hypertension
• List conditions by age group

Medication Queries:
• What medications are prescribed most often?
• Show me patients taking insulin
• List all blood pressure medications
• What's the average cost of medications?
• Find patients on multiple medications

Encounter Queries:
• How many encounters are there?
• Show me recent emergency visits
• What's the total cost of all encounters?
• Find inpatient encounters
• List encounters by type

Provider Queries:
• List all providers
• How many providers work at each hospital?
• Show me family practice providers
• Which providers see the most patients?
• Find providers by specialty

Financial Queries:
• What's the average encounter cost?
• Show me the most expensive procedures
• Calculate total healthcare costs
• Find patients with highest expenses
• Analyze costs by condition

Complex Queries:
• Find diabetic patients on insulin
• Show me patients with multiple chronic conditions
• Which conditions cost the most to treat?
• Analyze readmission patterns
• Find patients needing preventive care
"""
    print(help_text)


def print_model_info():
    """Print information about Gemini models"""
    model_info = """
Google Gemini Models Available:
--------------------------------

🌟 Gemini 1.5 Pro (RECOMMENDED - Currently in use)
   - Best for complex reasoning and long context
   - 1 million token context window
   - Excellent for multi-step queries
   - Best accuracy for Cypher generation
   - Free tier: 2 RPM, 32,000 TPM

⚡ Gemini 1.5 Flash
   - Faster responses, lower latency
   - Good for simpler queries
   - 1 million token context window
   - Free tier: 15 RPM, 1 million TPM

💎 Gemini 1.0 Pro
   - Standard model, good balance
   - 32K token context window
   - Free tier: 60 RPM

Why Gemini 1.5 Pro for This Project?
- Superior reasoning for complex healthcare queries
- Better understanding of database schemas
- More accurate Cypher query generation
- Handles multi-table relationships well
- Large context for schema and examples

To change model, edit the code:
self.llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash")
"""
    print(model_info)


def print_response(response):
    """Pretty print the chatbot response"""
    print("\n" + "="*70)
    print("📊 ANSWER:")
    print("="*70)
    print(response['answer'])
    
    if response['cypher_query']:
        print("\n" + "-"*70)
        print("🔍 Generated Cypher Query:")
        print("-"*70)
        print(response['cypher_query'])
    
    if response['raw_results'] and len(response['raw_results']) > 0:
        print("\n" + "-"*70)
        print("📈 Raw Results (first 5):")
        print("-"*70)
        for i, result in enumerate(response['raw_results'][:5], 1):
            print(f"{i}. {result}")
    
    print("="*70 + "\n")


def main():
    """Main chatbot loop"""
    # Initialize chatbot
    try:
        chatbot = SyntheaChatbot()
    except Exception as e:
        print(f"Failed to initialize chatbot: {e}")
        return
    
    # Print banner
    print_banner()
    
    # Main conversation loop
    while True:
        try:
            # Get user input
            question = input("💬 You: ").strip()
            
            # Handle empty input
            if not question:
                continue
            
            # Handle commands
            if question.lower() in ['exit', 'quit', 'bye']:
                print("\n👋 Thank you for using Synthea Healthcare Chatbot!")
                break
            
            elif question.lower() == 'help':
                print_help()
                continue
            
            elif question.lower() == 'models':
                print_model_info()
                continue
            
            elif question.lower() == 'stats':
                print("\n📊 Database Statistics:")
                print("="*50)
                stats = chatbot.get_database_stats()
                for stat in stats:
                    print(f"{stat['NodeType']}: {stat['Count']}")
                print("="*50 + "\n")
                continue
            
            elif question.lower() == 'samples':
                print("\n👥 Sample Patients:")
                print("="*70)
                samples = chatbot.get_sample_patients()
                for i, patient in enumerate(samples, 1):
                    print(f"{i}. {patient['FirstName']} {patient['LastName']} "
                          f"({patient['Gender']}, Born: {patient['BirthDate']})")
                print("="*70 + "\n")
                continue
            
            elif question.lower() == 'clear':
                os.system('cls' if os.name == 'nt' else 'clear')
                print_banner()
                continue
            
            elif question.lower() == 'schema':
                print("\n📋 Database Schema:")
                print("="*70)
                print(chatbot.schema)
                print("="*70 + "\n")
                continue
            
            # Process natural language question
            print("\n🤔 Gemini is thinking...")
            response = chatbot.ask(question)
            print_response(response)
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        
        except Exception as e:
            print(f"\n❌ Error: {str(e)}\n")
            continue


if __name__ == "__main__":
    main()
