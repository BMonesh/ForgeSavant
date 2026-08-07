const mongoose = require('mongoose');
const bcrypt = require('bcryptjs');

const userSchema = new mongoose.Schema({
  fullname: { type: String, required: true, trim: true, maxlength: 100 },
  email: { type: String, required: true, unique: true, lowercase: true, trim: true, maxlength: 254 },
  password: { type: String },
  analyticsConsent: { type: Boolean, default: false },
  analyticsConsentedAt: { type: Date, default: null },
  analyticsConsentVersion: { type: String, default: "1.0" },
});

// Hash the password before saving the user
userSchema.pre('save', async function (next) {
  if (this.password && (this.isModified('password') || this.isNew)) {
    const salt = await bcrypt.genSalt(10);
    this.password = await bcrypt.hash(this.password, salt);
  }
  next();
});

module.exports = mongoose.model('users', userSchema);
