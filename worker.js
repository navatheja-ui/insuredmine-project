const { workerData, parentPort } = require('worker_threads');
const fs = require('fs');
const csv = require('csv-parser');
const mongoose = require('mongoose');

// MongoDB connection in worker
mongoose.connect('mongodb://localhost:27017/insurance_db', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
})
.then(() => console.log('Worker connected to MongoDB'))
.catch(err => console.error('Worker MongoDB connection error:', err));

// Define Schemas in the worker
const AgentSchema = new mongoose.Schema({
  name: { type: String, required: true },
  agency_id: { type: String },
});

const UserSchema = new mongoose.Schema({
  firstname: { type: String, required: true },
  dob: { type: Date },
  address: { type: String },
  phone: { type: String },
  state: { type: String },
  zip: { type: String },
  email: { type: String },
  gender: { type: String },
  userType: { type: String },
  city: { type: String }
});

const AccountSchema = new mongoose.Schema({
  name: { type: String, required: true },
  type: { type: String },
  user_id: { type: mongoose.Schema.Types.ObjectId, ref: 'User' }
});

const CategorySchema = new mongoose.Schema({
  name: { type: String, required: true, unique: true }
});

const CarrierSchema = new mongoose.Schema({
  name: { type: String, required: true, unique: true }
});

const PolicySchema = new mongoose.Schema({
  policy_number: { type: String, required: true, unique: true },
  policy_start_date: { type: Date },
  policy_end_date: { type: Date },
  premium_amount: { type: Number },
  policy_type: { type: String },
  policy_mode: { type: Number },
  producer: { type: String },
  csr: { type: String },
  primary: { type: Boolean, default: false },
  applicant_id: { type: String },
  hasActive: { type: Boolean },
  premium_amount_written: { type: Number },
  user_id: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  account_id: { type: mongoose.Schema.Types.ObjectId, ref: 'Account' },
  category_id: { type: mongoose.Schema.Types.ObjectId, ref: 'Category' },
  carrier_id: { type: mongoose.Schema.Types.ObjectId, ref: 'Carrier' },
  agent_id: { type: mongoose.Schema.Types.ObjectId, ref: 'Agent' }
});

// Create models
const Agent = mongoose.model('Agent', AgentSchema);
const User = mongoose.model('User', UserSchema);
const Account = mongoose.model('Account', AccountSchema);
const Category = mongoose.model('Category', CategorySchema);
const Carrier = mongoose.model('Carrier', CarrierSchema);
const Policy = mongoose.model('Policy', PolicySchema);

// Process the CSV file
async function processFile() {
  const { filePath } = workerData;
  const results = [];
  
  const stats = {
    agents: 0,
    users: 0,
    accounts: 0,
    categories: 0,
    carriers: 0,
    policies: 0
  };

  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => results.push(data))
      .on('end', async () => {
        try {
          for (const row of results) {
            // 1. Process Agent
            let agent = await Agent.findOne({ name: row.agent });
            if (!agent) {
              agent = await Agent.create({
                name: row.agent,
                agency_id: row.agency_id || null
              });
              stats.agents++;
            }

            // 2. Process User
            let user = await User.findOne({ 
              firstname: row.firstname,
              email: row.email
            });
            
            if (!user) {
              user = await User.create({
                firstname: row.firstname,
                dob: row.dob ? new Date(row.dob) : null,
                address: row.address,
                phone: row.phone,
                state: row.state,
                zip: row.zip,
                email: row.email,
                gender: row.gender,
                userType: row.userType,
                city: row.city
              });
              stats.users++;
            }

            // 3. Process Account
            let account = await Account.findOne({ 
              name: row.account_name,
              user_id: user._id 
            });
            
            if (!account) {
              account = await Account.create({
                name: row.account_name,
                type: row.account_type,
                user_id: user._id
              });
              stats.accounts++;
            }

            // 4. Process Category (LOB)
            let category = await Category.findOne({ name: row.category_name });
            if (!category) {
              category = await Category.create({
                name: row.category_name
              });
              stats.categories++;
            }

            // 5. Process Carrier
            let carrier = await Carrier.findOne({ name: row.company_name });
            if (!carrier) {
              carrier = await Carrier.create({
                name: row.company_name
              });
              stats.carriers++;
            }

            // 6. Process Policy
            let policy = await Policy.findOne({ policy_number: row.policy_number });
            if (!policy) {
              policy = await Policy.create({
                policy_number: row.policy_number,
                policy_start_date: row.policy_start_date ? new Date(row.policy_start_date) : null,
                policy_end_date: row.policy_end_date ? new Date(row.policy_end_date) : null,
                premium_amount: parseFloat(row.premium_amount) || 0,
                premium_amount_written: parseFloat(row.premium_amount_written) || 0,
                policy_type: row.policy_type,
                policy_mode: parseInt(row.policy_mode) || null,
                producer: row.producer,
                csr: row.csr,
                primary: row.primary === 'true',
                applicant_id: row['Applicant ID'],
                hasActive: row.hasActive === 'true',
                user_id: user._id,
                account_id: account._id,
                category_id: category._id,
                carrier_id: carrier._id,
                agent_id: agent._id
              });
              stats.policies++;
            }
          }
          
          parentPort.postMessage(stats);
          
          // Close mongoose connection after processing
          await mongoose.connection.close();
        } catch (error) {
          console.error('Error processing data:', error);
          reject(error);
        }
      })
      .on('error', (error) => {
        console.error('Error reading file:', error);
        reject(error);
      });
  });
}

// Execute the file processing
processFile()
  .catch(err => {
    console.error('Worker error:', err);
    parentPort.postMessage({ error: err.message });
  });