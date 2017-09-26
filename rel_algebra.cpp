// Relational Algebra Implementation in C++
// Ishank Arora, 14074009
// Third Year CSE, IDD

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <stack>
#include <vector>
#include <map>
#include <iomanip>
#include <ctype.h>
#include <stdexcept>

using namespace std;

typedef vector <vector<string> > vvs;		// Declare aliases for the data structures used
typedef vector <string> vs;

int success;						// Global variable to store if the query is successfully executed or not

vs table_list {"students", "courses", "enrollment", "houses", "ip_addresses"};		// List of tables in the database

struct relation				// Structure to hold the table name, list of attributes, records and numerical mapping of the attributes
{
	string table_name;
    vvs records;
    map <string, int> att_map;
    vs att_list;
};

relation user_query(string query);

relation project(string query, relation data);						// Functions corresponding to the different operations
relation rename(string query, relation data);
relation select_util(string query, relation data);
relation select(string query, relation data);
relation union1(relation t1, relation t2);
relation intersection(relation t1, relation t2);
relation cartesian(relation t1, relation t2);
relation set_diff(relation t1, relation t2);
relation join(relation t1, relation t2);
relation division(relation t1, relation t2);
relation sel_max(string query, relation data);
relation sel_min(string query, relation data);
relation sel_avg(string query, relation data);
relation sel_sum(string query, relation data);
relation sel_count(string query, relation data);

// Converts a string to integer, returns -1 if the string cannot be converted
int to_int(string s)
{
    int dec;
    try
    {
        dec = stoi(s);
    }
    catch(std::invalid_argument& e)
    {
        return -1;
    }
    return dec;
}

// Strips trailing whitespaces from a string
string strip(string s)
{
    while(s[0]==' ')
        s.erase(s.begin());
    while(s[s.size()-1]==' ')
        s.erase(s.size()-1);
    return s;
}

// Removes pairs of brackets enclosing a string
string strip_b(string s)
{
    while(s[0]=='(' && s[s.size()-1]==')')
    {
        s.erase(s.size()-1);
        s.erase(s.begin());
    }
    return s;
}

// Splits a string into a vector of strings about the given delimiter
vs split(string s, char delim)
{
    vs cols;
    stringstream ss(s);
    string cell;
    while (getline(ss, cell, delim))
    {
        cell=strip(cell);
        cols.push_back(cell);
    }
    return cols;
}

// The Table class has the contents of the table as private members, so these cannot be accessed directly by the user
// The user can call the member function 'parse_query' with a specific query and obtain the results of its execution
class Table
{
    relation data;
    int att_count, row_count;			// stores the attribute and row count of the table

    public:
        Table(string t_name)			// Constructor initializes row and attribute count to 0 and then calls read_data
        {
            data.table_name=t_name;
            att_count=0;
            row_count=0;
            read_data("relations/" + t_name + ".csv");
        }

        void read_data(string file_name)		// Reads the attributes and records of the given table from csv file
        {
            ifstream f1(file_name);
            if(!f1.good())
            {
                success=0;
                return;
            }
            string line, cell;
            getline(f1, line);
            data.att_list = split(line, ',');
            for(; att_count<data.att_list.size(); att_count++)
                data.att_map[data.att_list[att_count]]=att_count;

            while(getline(f1, line))
            {
                data.records.push_back(split(line, ','));
                row_count++;
            }
        }

        relation parse_query(string query)		// Parses the query entered by user and calls the corresponding functions
        {
            stack <pair <char, string> > proc;		// Stack to store the type of operation and the corresponding arguments
            while(query!=data.table_name)		
            {
                int s1=query.find("[");
                int k=1, s2=s1;
                while(k>0)
                {
                    s2++;
                    if(query[s2]=='[')
                        k++;
                    else if(query[s2]==']')
                        k--;
                }

                string args=query.substr(s1+1, s2-s1-1);			// Arguments are defined inside square brackets []
                proc.push(make_pair(toupper(query[0]), args));
                s1=query.find("(", s2+1);
                s2=query.size()-1;
                while(query[s2]!=')')
                    s2--;
                query=query.substr(s1+1, s2-s1-1);		// After pushing one operation to the stack, the query is reduced to its input
                query=strip(query);
                query=strip_b(query);
            }
            relation out=data;
            while(!proc.empty())
            {
                pair <char, string> x=proc.top();		// Pop the operations from the stack
                proc.pop();
                //cout<<x.first<<"\t"<<x.second<<endl;
                
                // Depending on the first letter of the operation pushed in the stack, different functions are called
                if(x.first=='P')
                    out=project(x.second, out);
                else if(x.first=='S')
                    out=select_util(x.second, out);
                else if(x.first=='R')
                    out=rename(x.second, out);
                else if(x.first=='X')
                    out=sel_max(x.second, out);
                else if(x.first=='N')
                    out=sel_min(x.second, out);
                else if(x.first=='A')
                    out=sel_avg(x.second, out);
                else if(x.first=='T')
                    out=sel_sum(x.second, out);
                else if(x.first=='O')
                    out=sel_count(x.second, out);
                else if(x.first=='U')
                {
                    relation temp = user_query(x.second);
                    if(success)
                    	out = union1(temp, out);
                }
                else if(x.first=='I')
                {
                    relation temp = user_query(x.second);
                    if(success)
                    	out = intersection(temp, out);
                }
                else if(x.first=='C')
                {
                    relation temp = user_query(x.second);
                    if(success)
                    	out = cartesian(temp, out);
                }
                else if(x.first=='D')
                {
                    relation temp = user_query(x.second);
                    if(success)
                    	out = set_diff(temp, out);
                }
                else if(x.first=='J')
                {
                    relation temp = user_query(x.second);
                    if(success)
                    	out = join(temp, out);
                }
                else if(x.first=='V')
                {
                    relation temp = user_query(x.second);
                    if(success)
                    	out = division(temp, out);
                }
                else
                {
                    cout<<"ERROR: Invalid query\n\n";			// Undefined operation
                    success=0;
                }
            }
            return out;
        }

};

// Converts an infix expression involving predicates alongwith AND(^) and OR(|) symbols to postfix
vs ToPostfix(string query)
{
	stack<string> S;
	vs postfix;
	int i, j;
	for(i=0; i<query.size(); i++)
	{
		if(query[i]==' ')
			continue;
		else if(query[i]=='^' || query[i]=='|')
		{
			while(!S.empty() && S.top() != "(")			// Pops out elements until it encounters an opening parantheses
			{
				postfix.push_back(S.top());
				S.pop();
			}
			if(query[i]=='^')
				S.push("^");
			else
				S.push("|");
		}
		else if(query[i]=='(')
			S.push("(");
		else if(query[i]==')')
		{
			while(!S.empty() && S.top() !=  "(")
			{
				postfix.push_back(S.top());
				S.pop();
			}
			S.pop();
		}
		else 						// Finds the predicate until a logical operator or parantheses are found
		{
			j=i;
			while(query[i]!='^' && query[i]!='|' && query[i]!='(' && query[i]!=')' && i<query.size())
				i++;
			postfix.push_back(query.substr(j, i-j));
			i--;
		}

	}
	while(!S.empty())
	{
		postfix.push_back(S.top());
		S.pop();
	}

	return postfix;
}

// Removes duplicate records from the table
relation remove_dup(relation data)
{
    relation out;
    out.table_name=data.table_name;
    out.att_list=data.att_list;
    out.att_map=data.att_map;
    int flag;
    for(int j=0; j<data.records.size(); j++)
    {
        flag=0;
        for(int k=0; k<j; k++)
            if(data.records[j]==data.records[k])
            {
                flag=1;
                break;
            }
        if(!flag)
            out.records.push_back(data.records[j]);
    }
    return out;
}

// Prints the table with proper formatting
void print_table(relation out)
{
    cout<<"\n  ";
    vector <int> sz(out.att_list.size(), 0);
    int j, k, p=0;
    for(j=0; j<out.att_list.size(); j++)
    {
    	sz[j] = out.att_list[j].size();
    	for(k=0; k<out.records.size(); k++)
    	{
    		if(out.records[k][j].size()>sz[j])
    			sz[j]=out.records[k][j].size();
    	}
    	p+=sz[j];
    }
    for(j=0; j<out.att_list.size(); j++)
    {
        cout << left << setw(sz[j]+1) << setfill(' ') <<out.att_list[j]<<"  |  ";
    }
    cout<<"\n";
    for(j=0; j<6*out.att_list.size()+p; j++)
        cout<<"-";
    cout<<"\n  ";
    for(j=0; j<out.records.size(); j++)
    {
        for(k=0; k<out.records[j].size(); k++)
            cout << left << setw(sz[k]+1) << setfill(' ') <<out.records[j][k]<<"  |  ";
        cout<<"\n  ";
    }
    cout<<"\n";
}

// Utility function to implement multiple predicates in the select operation
relation select_util(string query, relation data)
{
	vs cols = ToPostfix(query);				// Converts given query to postfix
	vector <relation> r;
	int i, j, k, flag;
	for(i=0; i<cols.size(); i++)
	{
		if(cols[i]!="^" && cols[i]!="|")
			r.push_back(select(strip(cols[i]), data));			// First applies select operation to each of the predicates
		else
			r.push_back(data);
	}
	stack<relation> Stk;
	for(i=0; i<cols.size(); i++)
	{
		if(cols[i]=="^" || cols[i]=="|")
		{
			relation op1 = Stk.top();
			Stk.pop();
			relation op2 = Stk.top();
			Stk.pop();
			relation out;
			out.att_map=op1.att_map;
            out.att_list=op1.att_list;
            out.table_name=op1.table_name;
			if(cols[i]=="^")						// If ^ operator is found, include records common to both relations
			{
				for(j=0; j<op1.records.size(); j++)
				{
					flag=0;
					for(k=0; k<op2.records.size(); k++)
					{
						if(op1.records[j]==op2.records[k])
						{
							flag=1;
							break;
						}
					}
					if(flag)
						out.records.push_back(op1.records[j]);
				}
			}
			else 				// If | operator is found, take all the records from both tables and remove duplicates at the end
			{
				out=op1;
				out.records.insert(out.records.end(), op2.records.begin(), op2.records.end());
			}
			Stk.push(out);			// Pushes the result of the operation on the two relations to the stack
		}
		else
		{
			Stk.push(r[i]);
		}
	}
	return remove_dup(Stk.top());
}


// Selects the records from a relation based on a given predicate
relation select(string query, relation data)
{
    relation out;
    int i, isn=0;
    if(query.find(">=")!=string::npos)			// Greater than or equal to operator
    {
        query.erase(query.begin()+query.find(">="));
        vs cols=split(query, '=');
        if(cols[1][0]=='\'' && cols[1][cols[1].size()-1]=='\'')		// Checks if the value to be compared against is a constant or not
        {
            cols[1].erase(cols[1].begin());
            cols[1].erase(cols[1].size()-1);
            if(data.att_map.count(cols[0])==0)			// Displays error if column not present in the mapping
            {
                cout<<"ERROR: Column does not exist\n\n";
                success=0;
                return out;
            }
            if(to_int(cols[1])!=-1)
                isn=1;
            int c1=data.att_map[cols[0]];
            out.att_map=data.att_map;
            out.att_list=data.att_list;
            for(i=0; i<data.records.size(); i++)		// Selects records which obey the predicate
            {
                if(isn)
                {
                	if(to_int(data.records[i][c1])>=to_int(cols[1]))
                    	out.records.push_back(data.records[i]);
                }
                else if(data.records[i][c1]>=cols[1])
                    out.records.push_back(data.records[i]);
            }
        }		
        else 				// If second value is not a constant, it must be another column
        {
            if(data.att_map.count(cols[0])==0 || data.att_map.count(cols[1])==0)
            {
                cout<<"ERROR: Column does not exist\n\n";
                success=0;
                return out;
            }
            int c1=data.att_map[cols[0]], c2=data.att_map[cols[1]];
            out.att_map=data.att_map;
            out.att_list=data.att_list;
            if(to_int(data.records[0][c1])!=-1)
                isn=1;
            for(i=0; i<data.records.size(); i++)
            {
                if(isn)
                {
                	if(to_int(data.records[i][c1])>=to_int(data.records[i][c2]))
                    	out.records.push_back(data.records[i]);
                }
                else if(data.records[i][c1]>=data.records[i][c2])
                    out.records.push_back(data.records[i]);
            }
        }
    }
    else if(query.find("<=")!=string::npos)					// Less than or equal to operator
    {
        query.erase(query.begin()+query.find(">="));
        vs cols=split(query, '=');
        if(cols[1][0]=='\'' && cols[1][cols[1].size()-1]=='\'')
        {
            cols[1].erase(cols[1].begin());
            cols[1].erase(cols[1].size()-1);
            if(data.att_map.count(cols[0])==0)
            {
                cout<<"ERROR: Column does not exist\n\n";
                success=0;
                return out;
            }
            if(to_int(cols[1])!=-1)
                isn=1;
            int c1=data.att_map[cols[0]];
            out.att_map=data.att_map;
            out.att_list=data.att_list;
            for(i=0; i<data.records.size(); i++)
            {
                if(isn)
                {
                	if(to_int(data.records[i][c1])<=to_int(cols[1]))
                    	out.records.push_back(data.records[i]);
                }
                else if(data.records[i][c1]<=cols[1])
                    out.records.push_back(data.records[i]);
            }
        }
        else
        {
            if(data.att_map.count(cols[0])==0 || data.att_map.count(cols[1])==0)
            {
                cout<<"ERROR: Column does not exist\n\n";
                success=0;
                return out;
            }
            int c1=data.att_map[cols[0]], c2=data.att_map[cols[1]];
            out.att_map=data.att_map;
            out.att_list=data.att_list;
            if(to_int(data.records[0][c1])!=-1)
                isn=1;
            for(i=0; i<data.records.size(); i++)
            {
                if(isn)
                {
                	if(to_int(data.records[i][c1])<=to_int(data.records[i][c2]))
                    	out.records.push_back(data.records[i]);
                }
                else if(data.records[i][c1]<=data.records[i][c2])
                    out.records.push_back(data.records[i]);
            }
        }
    }
    else if(query.find("=")!=string::npos)					// Equal to operator
    {
        vs cols=split(query, '=');
        if(cols[1][0]=='\'' && cols[1][cols[1].size()-1]=='\'')
        {
            cols[1].erase(cols[1].begin());
            cols[1].erase(cols[1].size()-1);
            if(data.att_map.count(cols[0])==0)
            {
                cout<<"ERROR: Column does not exist\n\n";
                success=0;
                return out;
            }
            if(to_int(cols[1])!=-1)
                isn=1;
            int c1=data.att_map[cols[0]];
            out.att_map=data.att_map;
            out.att_list=data.att_list;
            for(i=0; i<data.records.size(); i++)
            {
                if(isn)
                {
                	if(to_int(data.records[i][c1])==to_int(cols[1]))
                    	out.records.push_back(data.records[i]);
                }
                else if(data.records[i][c1]==cols[1])
                    out.records.push_back(data.records[i]);
            }
        }
        else
        {
            if(data.att_map.count(cols[0])==0 || data.att_map.count(cols[1])==0)
            {
                cout<<"ERROR: Column does not exist\n\n";
                success=0;
                return out;
            }
            int c1=data.att_map[cols[0]], c2=data.att_map[cols[1]];
            out.att_map=data.att_map;
            out.att_list=data.att_list;
            if(to_int(data.records[0][c1])!=-1)
                isn=1;
            for(i=0; i<data.records.size(); i++)
            {
                if(isn)
                {
                	if(to_int(data.records[i][c1])==to_int(data.records[i][c2]))
                    	out.records.push_back(data.records[i]);
                }
                else if(data.records[i][c1]==data.records[i][c2])
                    out.records.push_back(data.records[i]);
            }
        }

    }
    else if(query.find("!")!=string::npos)					// Not equal to operator
    {
        vs cols=split(query, '!');
        if(cols[1][0]=='\'' && cols[1][cols[1].size()-1]=='\'')
        {
            cols[1].erase(cols[1].begin());
            cols[1].erase(cols[1].size()-1);
            if(data.att_map.count(cols[0])==0)
            {
                cout<<"ERROR: Column does not exist\n\n";
                success=0;
                return out;
            }
            if(to_int(cols[1])!=-1)
                isn=1;
            int c1=data.att_map[cols[0]];
            out.att_map=data.att_map;
            out.att_list=data.att_list;
            for(i=0; i<data.records.size(); i++)
            {
                if(isn)
                {
                	if(to_int(data.records[i][c1])!=to_int(cols[1]))
                    	out.records.push_back(data.records[i]);
                }
                else if(data.records[i][c1]!=cols[1])
                    out.records.push_back(data.records[i]);
            }
        }
        else
        {
            if(data.att_map.count(cols[0])==0 || data.att_map.count(cols[1])==0)
            {
                cout<<"ERROR: Column does not exist\n\n";
                success=0;
                return out;
            }
            int c1=data.att_map[cols[0]], c2=data.att_map[cols[1]];
            out.att_map=data.att_map;
            out.att_list=data.att_list;
            if(to_int(data.records[0][c1])!=-1)
                isn=1;
            for(i=0; i<data.records.size(); i++)
            {
                if(isn)
                {
                	if(to_int(data.records[i][c1])!=to_int(data.records[i][c2]))
                    	out.records.push_back(data.records[i]);
                }
                else if(data.records[i][c1]!=data.records[i][c2])
                    out.records.push_back(data.records[i]);
            }
        }
    }
    else if(query.find(">")!=string::npos)					// Greater than operator
    {
        vs cols=split(query, '>');
        if(cols[1][0]=='\'' && cols[1][cols[1].size()-1]=='\'')
        {
            cols[1].erase(cols[1].begin());
            cols[1].erase(cols[1].size()-1);
            if(data.att_map.count(cols[0])==0)
            {
                cout<<"ERROR: Column does not exist\n\n";
                success=0;
                return out;
            }
            if(to_int(cols[1])!=-1)
                isn=1;
            int c1=data.att_map[cols[0]];
            out.att_map=data.att_map;
            out.att_list=data.att_list;
            for(i=0; i<data.records.size(); i++)
            {
                if(isn)
                {
                	if(to_int(data.records[i][c1])>to_int(cols[1]))
                    	out.records.push_back(data.records[i]);
                }
                else if(data.records[i][c1]>cols[1])
                    out.records.push_back(data.records[i]);
            }
        }
        else
        {
            if(data.att_map.count(cols[0])==0 || data.att_map.count(cols[1])==0)
            {
                cout<<"ERROR: Column does not exist\n\n";
                success=0;
                return out;
            }
            int c1=data.att_map[cols[0]], c2=data.att_map[cols[1]];
            out.att_map=data.att_map;
            out.att_list=data.att_list;
            if(to_int(data.records[0][c1])!=-1)
                isn=1;
            for(i=0; i<data.records.size(); i++)
            {
                if(isn)
                {
                	if(to_int(data.records[i][c1])>to_int(data.records[i][c2]))
                    	out.records.push_back(data.records[i]);
                }
                else if(data.records[i][c1]>data.records[i][c2])
                    out.records.push_back(data.records[i]);
            }
        }
    }
    else if(query.find("<")!=string::npos)					// Less than or equal to operator
    {
        vs cols=split(query, '<');
        if(cols[1][0]=='\'' && cols[1][cols[1].size()-1]=='\'')
        {
            cols[1].erase(cols[1].begin());
            cols[1].erase(cols[1].size()-1);
            if(data.att_map.count(cols[0])==0)
            {
                cout<<"ERROR: Column does not exist\n\n";
                success=0;
                return out;
            }
            if(to_int(cols[1])!=-1)
                isn=1;
            int c1=data.att_map[cols[0]];
            out.att_map=data.att_map;
            out.att_list=data.att_list;
            for(i=0; i<data.records.size(); i++)
            {
                if(isn)
                {
                	if(to_int(data.records[i][c1])<to_int(cols[1]))
                    	out.records.push_back(data.records[i]);
                }
                else if(data.records[i][c1]<cols[1])
                    out.records.push_back(data.records[i]);
            }
        }
        else
        {
            if(data.att_map.count(cols[0])==0 || data.att_map.count(cols[1])==0)
            {
                cout<<"ERROR: Column does not exist\n\n";
                success=0;
                return out;
            }
            int c1=data.att_map[cols[0]], c2=data.att_map[cols[1]];
            out.att_map=data.att_map;
            out.att_list=data.att_list;
            if(to_int(data.records[0][c1])!=-1)
                isn=1;
            for(i=0; i<data.records.size(); i++)
            {
                if(isn)
                	if(to_int(data.records[i][c1])<to_int(data.records[i][c2]))
                    	out.records.push_back(data.records[i]);
                else if(data.records[i][c1]<data.records[i][c2])
                    out.records.push_back(data.records[i]);
            }
        }
    }
    else 				// Displays error if none of the operators are present
    {
        cout<<"ERROR: Not enough arguments\n\n";
        success=0;
        return out;
    }
    return remove_dup(out);
}


// Takes projection of a given relation
relation project(string query, relation data)
{
    relation out;
    vs cols=split(query, ',');
    for(int i=0; i<cols.size(); i++)
    {
        if(data.att_map.count(cols[i])==0)			// If a column is not present in relation, displays the error
        {
            cout<<"ERROR: Column does not exist\n\n";
            success=0;
            return out;
        }
        out.att_map[cols[i]]=i;
        out.att_list.push_back(cols[i]);
    }
    out.table_name=data.table_name;
    for(int i=0; i<data.records.size(); i++)
    {
        vs temp;
        for(int j=0; j<cols.size(); j++)			// Inserts only the required attributes of the records
            temp.push_back(data.records[i][data.att_map[cols[j]]]);
        out.records.push_back(temp);
    }
    return remove_dup(out);
}

// Renames the relation, as well as the columns of the relation
relation rename(string query, relation data)
{
    int s1=query.find("("), s2=query.find(")");
    relation out;
    if((s1!=string::npos && s2==string::npos) || (s1==string::npos && s2!=string::npos))
    {
        cout<<"ERROR: Invalid query\n\n";
        success=0;
    }
    else if(s1==string::npos && s2==string::npos)		// Column names not provided, so renames the table only
    {
        out=data;
        out.table_name = strip(query);
    }
    else
    {
        int i=0;
        while(query[i]==' ')
            i++;
        if(i==s1)
        {
            success=0;
            cout<<"ERROR: Name for table not entered\n\n";		// Displays the error if the new table name is not entered with the column names
            return out;
        }
        out.table_name = strip(query.substr(0, s1));
        query=query.substr(s1+1, s2-s1-1);
        vs cols=split(query, ',');
        for(i=0; i<cols.size(); i++)
            if(cols[i]=="")
                cols.erase(cols.begin()+i);

        // Displays error if the number of new column names does not match with the number of original column names
        if(cols.size()!=data.att_list.size())
        {
            cout<<"ERROR: Number of columns do not match\n\n";
            success=0;
        }
        else
        {
            out.records=data.records;
            out.att_list=cols;
            for(i=0; i<cols.size(); i++)
                out.att_map[cols[i]]=i;
        }
    }
    return remove_dup(out);
}

// Takes union of two relations
relation union1(relation t1, relation t2)
{
    relation out;
    if(t1.att_list!=t2.att_list)			// The attribute list of the relations must be the same
    {
        cout<<"ERROR: The tables are not union compatible\n\n";
        success=0;
        return out;
    }
    out.table_name=t1.table_name+" U "+t2.table_name;
    out.att_list=t1.att_list;
    out.att_map=t1.att_map;
    out.records=t1.records;
    out.records.insert(out.records.end(), t2.records.begin(), t2.records.end());		// Concatenates the records
    return remove_dup(out);					// Removes duplicates
}

// Takes intersection of two relations
relation intersection(relation t1, relation t2)
{
    relation out;
    if(t1.att_list!=t2.att_list)			// The attribute list of the relations must be the same
    {
        cout<<"ERROR: The tables are not union compatible\n\n";			
        success=0;
        return out;
    }
    out.table_name=t1.table_name+" I "+t2.table_name;
    out.att_list=t1.att_list;
    out.att_map=t1.att_map;
    int flag;
    for(int i=0; i<t1.records.size(); i++)
    {
        flag=0;
        for(int j=0; j<t2.records.size(); j++)
        {
            if(t1.records[i]==t2.records[j])
            {
                flag=1;
                break;
            }
        }
        if(flag)				// Inserts a record only if it exists in both relations
            out.records.push_back(t1.records[i]);

    }
    return remove_dup(out);
}

// Takes set difference of two relations
relation set_diff(relation t1, relation t2)
{
    relation out;
    if(t1.att_list!=t2.att_list)		// The attribute list of the relations must be the same
    {
        cout<<"ERROR: The tables are not union compatible\n\n";
        success=0;
        return out;
    }
    out.table_name=t1.table_name+" - "+t2.table_name;
    out.att_list=t1.att_list;
    out.att_map=t1.att_map;
    int flag;
    for(int i=0; i<t1.records.size(); i++)
    {
        flag=0;
        for(int j=0; j<t2.records.size(); j++)
        {
            if(t1.records[i]==t2.records[j])
            {
                flag=1;
                break;
            }
        }
        if(!flag)			// Inserts a record only if it exists in the first relation but not the second
            out.records.push_back(t1.records[i]);

    }
    return remove_dup(out);
}

// Takes cartesian product of two relations
relation cartesian(relation t1, relation t2)
{
    relation out;
    out.table_name=t1.table_name+" X "+t2.table_name;
    int dup, i, j;
    for(i=0; i<t1.att_list.size(); i++)
    {
    	dup=0;
    	for(j=0; j<t2.att_list.size(); j++)
    	{
    		if(t1.att_list[i]==t2.att_list[j])			// Checks if columns of same names exist in the two relations
    		{
    			dup=1;
    			break;
    		}
    	}
    	if(dup)
    	{
    		if(t1.table_name==t2.table_name)		// If column names in the two relations are same, the names of the relation should be different
    		{
    			cout<<"ERROR: Tables with same column names should have different table names\n\n";
		        success=0;
		        return out;
    		}
    		out.att_list.push_back(t1.table_name+"."+t1.att_list[i]);		// Makes the column names as table_name.column_name
    	}
    	else
    		out.att_list.push_back(t1.att_list[i]);
    }
    for(i=0; i<t2.att_list.size(); i++)				// Performs the checks for attributes in the second relation
    {
    	dup=0;
    	for(j=0; j<t1.att_list.size(); j++)
    	{
    		if(t2.att_list[i]==t1.att_list[j])
    		{
    			dup=1;
    			break;
    		}
    	}
    	if(dup)
    		out.att_list.push_back(t2.table_name+"."+t2.att_list[i]);
    	else
    		out.att_list.push_back(t2.att_list[i]);
    }
    for(i=0; i<out.att_list.size(); i++)
        out.att_map[out.att_list[i]] = i;
    for(i=0; i<t1.records.size(); i++)
    {
        for(j=0; j<t2.records.size(); j++)
        {
            vs temp = t1.records[i];
            temp.insert(temp.end(), t2.records[j].begin(), t2.records[j].end());		// Concatenates the records of the two relations
            out.records.push_back(temp);
        }
    }
    return remove_dup(out);
}


// Takes join of the two relations
relation join(relation t1, relation t2)
{
    relation out;
    out.table_name=t1.table_name+" J "+t2.table_name;
    out.att_list=t1.att_list;
    out.att_list.insert(out.att_list.end(), t2.att_list.begin(), t2.att_list.end());
    vector <pair<int, int> > comm;
    int i, j, k, flag;
    for(i=0; i<t1.att_list.size(); i++)
    {
    	if(t2.att_map.count(t1.att_list[i])!=0)				// Stores the mapping of columns with the same names in the two relations
    		comm.push_back(make_pair(i, t2.att_map[t1.att_list[i]]));
    }
    for(i=0; i<t1.records.size(); i++)
    {
        for(j=0; j<t2.records.size(); j++)
        {
            flag=1;
            for(k=0; k<comm.size(); k++)
            {
            	// Checks that the records have same value for all the common attributes
                if(t1.records[i][comm[k].first]!=t2.records[j][comm[k].second])
                {
                    flag=0;
                    break;
                }
            }
            if(flag)
            {
                vs temp = t1.records[i];
                temp.insert(temp.end(), t2.records[j].begin(), t2.records[j].end());
                out.records.push_back(temp);
            }
        }
    }
    for(i=0; i<out.att_list.size(); i++)
    {
        for(j=i+1; j<out.att_list.size(); j++)
        {
            if(out.att_list[i]==out.att_list[j])
            {
                out.att_list.erase(out.att_list.begin()+j);
                for(k=0; k<out.records.size(); k++)
                    out.records[k].erase(out.records[k].begin()+j);			// Removes the attributes repeated twice
            }
        }
    }
    for(i=0; i<out.att_list.size(); i++)			// Maps the attribute names to column numbers
        out.att_map[out.att_list[i]] = i;
    return remove_dup(out);
}

// Takes division of the two relations
relation division(relation t1, relation t2)
{
    int i, j, k, flag;

    // cols stores the list of attributes in relation 1 but not in relation 2 separated by commas
    string cols="";
    for(i=0; i<t1.att_list.size(); i++)
    {
    	if(t2.att_map.count(t1.att_list[i])==0)
    		cols = cols+t1.att_list[i]+",";
    }
    cols.erase(cols.begin()+cols.size()-1);
    string cols1=cols;
    for(i=0; i<t2.att_list.size(); i++)			// cols1 stores list of attributes only in relation 1 followed by those in relation 2
    	cols1 = cols1+","+t2.att_list[i];
    relation temp1 = project(cols, t1);
    relation temp2 = project(cols, set_diff(cartesian(temp1, t2), project(cols1, t1)));
    relation out = set_diff(temp1, temp2);
    out.table_name=t1.table_name+" / "+t2.table_name;
    return out;
}

// Returns the maximum value of an attribute in the relation
relation sel_max(string query, relation data)
{
    relation out;
    out.table_name=data.table_name;
    if(data.att_map.count(query)==0)
    {
        cout<<"ERROR: Column does not exist\n\n";
        success=0;
        return out;
    }
    out.att_list.push_back("max("+query+")");
    out.att_map["max("+query+")"] = 0;				// Set column name as max(column_name)
    int i, isn=1, c = data.att_map[query];
    string ans = data.records[0][c];
    for(i=0; i<data.records.size(); i++)			// Check if the attribute if integral or not
    {
    	if(to_int(data.records[i][c])==-1)
    	{
    		isn=0;
    		break;
    	}
    }
    for(i=0; i<data.records.size(); i++)
    {
        if(isn)						// If column has integer values, convert the records before comparison
        {
        	if(to_int(data.records[i][c])>to_int(ans))
            	ans = data.records[i][c];
        }
        else if(data.records[i][c]>ans)
            ans = data.records[i][c];
    }
    vs temp {ans};
    out.records.push_back(temp);
    return out;
}

// Returns the minimum value of an attribute in the relation
relation sel_min(string query, relation data)
{
    relation out;
    out.table_name=data.table_name;
    if(data.att_map.count(query)==0)
    {
        cout<<"ERROR: Column does not exist\n\n";
        success=0;
        return out;
    }
    out.att_list.push_back("min("+query+")");
    out.att_map["min("+query+")"] = 0;
    int i, isn=1, c = data.att_map[query];
    string ans = data.records[0][c];
    for(i=0; i<data.records.size(); i++)			// Check if the attribute if integral or not
    {
    	if(to_int(data.records[i][c])==-1)
    	{
    		isn=0;
    		break;
    	}
    }
    for(i=0; i<data.records.size(); i++)
    {
        if(isn)
        {
        	if(to_int(data.records[i][c])<to_int(ans))
            	ans = data.records[i][c];
        }
        else if(data.records[i][c]<ans)
            ans = data.records[i][c];
    }
    vs temp {ans};
    out.records.push_back(temp);
    return out;
}

// Returns the average value of a numerical attribute in the relation
relation sel_avg(string query, relation data)
{
    relation out;
    out.table_name=data.table_name;
    if(data.att_map.count(query)==0)
    {
        cout<<"ERROR: Column does not exist\n\n";
        success=0;
        return out;
    }
    int i, c = data.att_map[query];
    float ans=0;
    for(i=0; i<data.records.size(); i++)
    {
        if(to_int(data.records[i][c])==-1)			// Displays error if the column has non-numeric values
        {
            cout<<"ERROR: Column contains non-numeric values\n\n";
            success=0;
            return out;
        }
        ans+=to_int(data.records[i][c]);
    }
    ans /= (float)data.records.size();
    out.att_list.push_back("avg("+query+")");
    out.att_map["avg("+query+")"] = 0;
    vs temp {to_string(ans)};
    out.records.push_back(temp);
    return out;
}

// Returns the sum of the values of a numerical attribute in the relation
relation sel_sum(string query, relation data)
{
    relation out;
    out.table_name=data.table_name;
    if(data.att_map.count(query)==0)
    {
        cout<<"ERROR: Column does not exist\n\n";
        success=0;
        return out;
    }
    int i, c = data.att_map[query], ans=0;
    for(i=0; i<data.records.size(); i++)
    {
        if(to_int(data.records[i][c])==-1)			// Displays error if the column has non-numeric values
        {
            cout<<"ERROR: Column contains non-numeric values\n\n";
            success=0;
            return out;
        }
        ans+=to_int(data.records[i][c]);
    }
    out.att_list.push_back("sum("+query+")");
    out.att_map["sum("+query+")"] = 0;
    vs temp {to_string(ans)};
    out.records.push_back(temp);
    return out;
}

// Returns the count of an attribute in the relation
relation sel_count(string query, relation data)
{
    relation out;
    out.table_name=data.table_name;
    if(data.att_map.count(query)==0)
    {
        cout<<"ERROR: Column does not exist\n\n";
        success=0;
        return out;
    }
    int i, c = data.att_map[query], ans=0;
    for(i=0; i<data.records.size(); i++)
    {
        if(data.records[i][c]!="")			// Not included in count if attribute is empty
            ans++;
    }
    out.att_list.push_back("count("+query+")");
    out.att_map["count("+query+")"] = 0;
    vs temp {to_string(ans)};
    out.records.push_back(temp);
    return out;
}

// Function to determine the table being called in the user query and create its corresponding Table object
relation user_query(string query)
{
    string temp=query;
    int s1, s2, p1, p2, k, i, flag=1;
    while(temp.find("[")!=string::npos)			// Removes the blocks contained in square brackets
    {
        s1 = temp.find("[");
        k=1;
        s2=s1;
        while(k>0 && s2<temp.size())
        {
            s2++;
            if(temp[s2]=='[')
                k++;
            else if(temp[s2]==']')
                k--;
        }
        if((s1!=string::npos && s2==temp.size())||(s1==string::npos && s2!=temp.size()))
        {
            flag=0;
            break;
        }
        temp.erase(temp.begin()+s1, temp.begin()+s2+1);
    }
    relation out;
    if(flag==0)
    {
        cout<<"ERROR: Invalid query\n\n";
        success = 0;
        return out;
    }
    while(temp.find("(")!=string::npos)			// Removes the blocks contained in paranthesis
    {
        s1 = temp.find("(");
        k=1;
        s2=s1;
        while(k>0 && s2<temp.size())
        {
            s2++;
            if(temp[s2]=='(')
                k++;
            else if(temp[s2]==')')
                k--;
        }
        if((s1!=string::npos && s2==temp.size())||(s1==string::npos && s2!=temp.size()))
        {
            flag=0;
            break;
        }
        if(temp[0]!='(' && s1!=string::npos)
            temp.erase(temp.begin());
        else if(temp[0]=='(')
            temp=strip_b(temp);
    }
    if(flag==0)			// Displays error if non-matching square brackets or parantheses are present
    {
        cout<<"ERROR: Invalid query\n\n";
        success = 0;
        return out;
    }
    //cout<<temp;
    flag=0;
    for(i=0; i<table_list.size(); i++)
    {
        if(table_list[i]==temp)				// If table name if found in table_list, creates an object with the given table name
        {
            Table A(table_list[i]);
            if(!success)
                continue;
            flag=1;
            query=strip(query);
            query=strip_b(query);
            out = A.parse_query(query);
            break;
        }
    }
    if(!flag)				// Displays error if table name is not present in table_list or csv file with the table name does not exist
    {
        cout<<"ERROR: Table does not exist\n\n";
        success=0;
    }
    return out;
}

int main()
{
    cout<<"\nUsage:\n";
    cout<<"> Select operation: S[predicate](relation_name)\n";
    cout<<"> Project operation: P[column_list](relation_name)\n";
    cout<<"> Rename operation: R[new_relation_name(new_column_names)](relation_name)\n";
    cout<<"> Union operation: U[relation_1_name](relation_2_name)\n";
    cout<<"> Intersection operation: I[relation_1_name](relation_2_name)\n";
    cout<<"> Cartesian Product operation: C[relation_1_name](relation_2_name)\n";
    cout<<"> Set Difference operation: D[relation_1_name](relation_2_name)\n";
    cout<<"> Join operation: J[relation_1_name](relation_2_name)\n";
    cout<<"> Division operation: V[relation_1_name](relation_2_name)\n";
    cout<<"> Select Maximum operation: X[column_name](relation_name)\n";
    cout<<"> Select Minimum operation: N[column_name](relation_name)\n";
    cout<<"> Select Sum operation: T[column_name](relation_name)\n";
    cout<<"> Select Average operation: A[column_name](relation_name)\n";
    cout<<"> Select Count operation: O[column_name](relation_name)\n\n";	
    string query="";
    while(true)
    {
        cout<<">> ";
        getline(cin, query);			// Get query from the user
        if(query=="exit")				// Breaks the loop if the user enters exit
            break;
        else if(query=="")
            continue;
        success = 1;					// Initially set the global variable success to 1
        relation out = user_query(query);		// Parsing and executing the user querys
        if(success)						// Print the table only if execution is successful, else corresponding error message is displayed
            print_table(out);
    }
    return 0;
}
