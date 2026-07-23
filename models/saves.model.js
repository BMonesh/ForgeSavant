const mongoose = require('mongoose');

const savesSchema = new mongoose.Schema({
  cpu : { type : String, required : true},
  motherboard : { type : String, required : true}, 
  gpu : { type : String, required : true},
  primaryStorage : { type : String, required : true},
  secondaryStorage : { type : String, default: ""},
  ram : { type : String, required : true},
  powerSupply : { type : String, required : true},
  cabinet : { type : String, required : true},
  email : { type : String, required : true},
  cinebench : {type: String},
  cyberpunk : {type: String},
  analytics: { type: mongoose.Schema.Types.Mixed },
  image: {type: String, default: ""},
  componentIds: { type: mongoose.Schema.Types.Mixed },
  compatibility: { type: mongoose.Schema.Types.Mixed },
  compatibilityEngineVersion: { type: String },
  verifiedAt: { type: Date },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
});

const savesSchema2 = new mongoose.Schema({
  _id: {type: mongoose.Schema.Types.ObjectId, required: true},
  cpu : { type : String, required : true},
  motherboard : { type : String, required : true},
  gpu : { type : String, required : true},
  primaryStorage : { type : String, required : true},
  secondaryStorage : { type : String, default: ""},
  ram : { type : String, required : true},
  powerSupply : { type : String, required : true},
  cabinet : { type : String, required : true},
  email : { type : String, required : true},
  cinebench : {type: String},
  cyberpunk : {type: String},
  analytics: { type: mongoose.Schema.Types.Mixed },
  image: {type: String, default: ""},
  componentIds: { type: mongoose.Schema.Types.Mixed },
  compatibility: { type: mongoose.Schema.Types.Mixed },
  compatibilityEngineVersion: { type: String },
  verifiedAt: { type: Date },
  createdAt: { type: Date },
  updatedAt: { type: Date },
})

const saves = mongoose.model('saves', savesSchema);
const saves2 = mongoose.model('saves2', savesSchema2, 'saves');
module.exports = {saves, saves2};
