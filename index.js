const express = require('express');
const mongoose = require('mongoose');
const multer = require('multer');
const csv = require('csv-parser');
const fs = require('fs');
const { Worker } = require('worker_threads');
const path = require('path');
const cron = require('node-cron');
const dotenv = require('dotenv')

dotenv.config();

const app = express();
app.use(express.json());

// MongoDB connection
mongoose.connect('mongodb://localhost:27017/insurance_db', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
})
.then(() => console.log('MongoDB connected'))
.catch(err => console.error('MongoDB connection error:', err));

//===================================
// TASK 1: Insurance CSV Processing
//===================================

// Define Schemas
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

// Configure multer for file upload
const upload = multer({ dest: 'uploads/' });

// Worker thread function for processing CSV data
function processCSV(filePath) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(path.join(__dirname, 'worker.js'), {
      workerData: { filePath }
    });

    worker.on('message', resolve);
    worker.on('error', reject);
    worker.on('exit', (code) => {
      if (code !== 0) {
        reject(new Error(`Worker stopped with exit code ${code}`));
      }
    });
  });
}

// API 1: Upload CSV/XLSX file
app.post('/api/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    // Process file with worker thread
    const result = await processCSV(req.file.path);
    
    // Clean up the uploaded file
    fs.unlinkSync(req.file.path);
    
    res.status(200).json({ 
      message: 'File processed successfully', 
      stats: result 
    });
  } catch (error) {
    console.error('Upload error:', error);
    res.status(500).json({ error: error.message });
  }
});

// API 2: Search policy by username
app.get('/api/policies/search', async (req, res) => {
  try {
    const { username } = req.query;
    
    if (!username) {
      return res.status(400).json({ error: 'Username is required' });
    }
    
    // Find user by firstname
    const user = await User.findOne({ 
      firstname: new RegExp(username, 'i') 
    });
    
    if (!user) {
      return res.status(404).json({ error: 'User not found' });
    }
    
    // Find policies for this user
    const policies = await Policy.find({ user_id: user._id })
      .populate('category_id', 'name')
      .populate('carrier_id', 'name')
      .populate('account_id', 'name type');
    
    res.status(200).json({ user, policies });
  } catch (error) {
    console.error('Search error:', error);
    res.status(500).json({ error: error.message });
  }
});

// API 3: Aggregated policies by user
app.get('/api/policies/aggregate', async (req, res) => {
  try {
    // Get all users with their policies aggregated
    const userPolicies = await User.aggregate([
      {
        $lookup: {
          from: 'policies',
          localField: '_id',
          foreignField: 'user_id',
          as: 'policies'
        }
      },
      {
        $project: {
          _id: 1,
          firstname: 1,
          email: 1,
          phone: 1,
          policy_count: { $size: '$policies' },
          total_premium: { $sum: '$policies.premium_amount' },
          policies: {
            $map: {
              input: '$policies',
              as: 'policy',
              in: {
                policy_number: '$$policy.policy_number',
                policy_type: '$$policy.policy_type',
                premium_amount: '$$policy.premium_amount',
                policy_start_date: '$$policy.policy_start_date',
                policy_end_date: '$$policy.policy_end_date'
              }
            }
          }
        }
      }
    ]);
    
    res.status(200).json(userPolicies);
  } catch (error) {
    console.error('Aggregation error:', error);
    res.status(500).json({ error: error.message });
  }
});

//===================================
// TASK 2: Scheduled Post Service
//===================================

// Define Schema for scheduled messages
const ScheduledPostSchema = new mongoose.Schema({
  message: { 
    type: String, 
    required: true 
  },
  scheduledDate: { 
    type: Date, 
    required: true 
  },
  status: { 
    type: String, 
    enum: ['pending', 'delivered', 'failed'], 
    default: 'pending' 
  },
  createdAt: { 
    type: Date, 
    default: Date.now 
  }
});

const ScheduledPost = mongoose.model('ScheduledPost', ScheduledPostSchema);

// API to schedule a message
app.post('/api/schedule-post', async (req, res) => {
  try {
    const { message, day, time } = req.body;
    
    // Validate input
    if (!message || !day || !time) {
      return res.status(400).json({ 
        error: 'Message, day, and time are required fields' 
      });
    }
    
    // Parse the scheduled date and time
    // Expected format: day (YYYY-MM-DD), time (HH:MM)
    const [year, month, date] = day.split('-').map(Number);
    const [hours, minutes] = time.split(':').map(Number);
    
    // Create Date object (month is 0-indexed in JavaScript)
    const scheduledDate = new Date(year, month - 1, date, hours, minutes);
    
    // Check if the date is valid and in the future
    const now = new Date();
    if (isNaN(scheduledDate) || scheduledDate <= now) {
      return res.status(400).json({ 
        error: 'Invalid or past date/time. Please provide a future date and time.' 
      });
    }
    
    // Create a new scheduled post
    const scheduledPost = new ScheduledPost({
      message,
      scheduledDate
    });
    
    await scheduledPost.save();
    
    res.status(201).json({
      message: 'Post scheduled successfully',
      scheduledPost
    });
  } catch (error) {
    console.error('Schedule post error:', error);
    res.status(500).json({ error: error.message });
  }
});

// API to get all scheduled posts
app.get('/api/scheduled-posts', async (req, res) => {
  try {
    const posts = await ScheduledPost.find().sort({ scheduledDate: 1 });
    res.status(200).json(posts);
  } catch (error) {
    console.error('Get scheduled posts error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Function to deliver scheduled posts
async function deliverScheduledPosts() {
  try {
    const now = new Date();
    // Find posts that are scheduled for now or earlier and are still pending
    const pendingPosts = await ScheduledPost.find({
      scheduledDate: { $lte: now },
      status: 'pending'
    });
    

    
    for (const post of pendingPosts) {
      try {
        // Here you would implement your actual message delivery logic
        // For example, send an email, push notification, etc.
        console.log(`Delivering post: ${post._id}, Message: ${post.message}`);
        
        // For demonstration, we'll just mark it as delivered
        post.status = 'delivered';
        await post.save();
        
        console.log(`Post ${post._id} delivered successfully`);
      } catch (err) {
        console.error(`Error delivering post ${post._id}:`, err);
        post.status = 'failed';
        await post.save();
      }
    }
  } catch (error) {
    console.error('Error in scheduled delivery:', error);
  }
}

// Set up a cron job to check for posts to deliver every minute
cron.schedule('* * * * *', deliverScheduledPosts);

// Start the server
const PORT = process.env.PORT || 3001;
const server = app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

// Export server for CPU monitor
module.exports = server;