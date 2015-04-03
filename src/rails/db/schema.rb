# encoding: UTF-8
# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# Note that this schema.rb definition is the authoritative source for your
# database schema. If you need to create the application database on another
# system, you should be using db:schema:load, not running all the migrations
# from scratch. The latter is a flawed and unsustainable approach (the more migrations
# you'll amass, the slower it'll run and the greater likelihood for issues).
#
# It's strongly recommended to check this file into your version control system.

ActiveRecord::Schema.define(:version => 20150403001531) do

  create_table "job_stats", :force => true do |t|
    t.integer  "job_id",        :limit => 8,                :null => false
    t.integer  "irc_id",        :limit => 8,                :null => false
    t.integer  "field_id",      :limit => 8,                :null => false
    t.integer  "count_success", :limit => 8, :default => 0
    t.integer  "count_failure", :limit => 8, :default => 0
    t.integer  "count_total",   :limit => 8, :default => 0
    t.datetime "created_at",                                :null => false
    t.datetime "updated_at",                                :null => false
  end

  add_index "job_stats", ["job_id", "irc_id", "field_id"], :name => "job_stats_index", :unique => true

end
