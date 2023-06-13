# -*- coding: utf-8 -*-

from collections import defaultdict
from datetime import timedelta, datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd
from pytz import utc
from odoo import models, fields, api, _
from odoo.http import request
from odoo.tools import float_utils

ROUNDING_FACTOR = 16


class HrLeave(models.Model):
    _inherit = 'hr.leave'
    duration_display = fields.Char('Requested (Days/Hours)', compute='_compute_duration_display', store=True,
                                   help="Field allowing to see the leave request duration"
                                        " in days or hours depending on the leave_type_request_unit")


class HrContract(models.Model):
    _inherit = 'hr.contract'

    @api.model
    def salary_range(self):
        data = []

        salary_ranges = [
            ('Below 25k', (None, 25000)),
            ('25k to 50k', (25000, 50000)),
            ('50k to 75k', (50000, 75000)),
            ('75k to 1 lakh', (75000, 100000)),
            ('above 1 lakh', (100000, None))
        ]

        for label, (min_salary, max_salary) in salary_ranges:
            domain = []
            if min_salary:
                domain.append(('wage', '>=', str(min_salary)))
            if max_salary:
                domain.append(('wage', '<', str(max_salary)))

            count = self.env['hr.contract'].search_count(domain)
            data.append({'label': label, 'value': count})

        return data

    # @api.model
    # def salary_range(self):
    #
    #     data = []
    #
    #     salary_below_25k = self.env['hr.contract'].search_count([('wage', '<', '25000')])
    #     salary_25k_50k = self.env['hr.contract'].search_count([('wage', '>=', '25000'), ('wage', '<', '50000')])
    #     salary_50k_75k = self.env['hr.contract'].search_count([('wage', '>=', '50000'), ('wage', '<', '75000')])
    #     salary_75k_1l = self.env['hr.contract'].search_count([('wage', '>=', '75000'), ('wage', '<', '100000')])
    #     salary_above_1l = self.env['hr.contract'].search_count([('wage', '>=', '100000')])
    #
    #     data.append({'label': 'Below 25k', 'value': salary_below_25k})
    #     data.append({'label': '25k to 50k', 'value': salary_25k_50k})
    #     data.append({'label': '50k to 75k', 'value': salary_50k_75k})
    #     data.append({'label': '75k to 1 lakh', 'value': salary_75k_1l})
    #     data.append({'label': 'above 1 lakh', 'value': salary_above_1l})
    #
    #     return data

    # @api.model
    # def avg_salary_emp(self):
    #
    #     a = self.env['hr.contract'].search([])
    #     total_employee = self.env['hr.contract'].search_count([])
    #     total_salary = 0
    #     for i in a:
    #         total_salary += i.wage
    #     print(total_salary)
    #     avg_salary = total_salary / total_employee
    #     print(avg_salary)
    #
    #     return avg_salary


class Employee(models.Model):
    _inherit = 'hr.employee'

    birthday = fields.Date('Date of Birth', groups="base.group_user", help="Birthday")

    @api.model
    def check_user_group(self):
        uid = request.session.uid
        user = self.env['res.users'].sudo().search([('id', '=', uid)], limit=1)
        if user.has_group('hr.group_hr_manager'):
            return True
        else:
            return False

    @api.model
    def get_user_employee_details(self):
        uid = request.session.uid
        employee = self.env['hr.employee'].sudo().search_read([('user_id', '=', uid)], limit=1)
        leaves_to_approve = self.env['hr.leave'].sudo().search_count([('state', 'in', ['confirm', 'validate1'])])
        today = datetime.strftime(datetime.today(), '%Y-%m-%d')
        query = """
        select count(id)
        from hr_leave
        WHERE (hr_leave.date_from::DATE,hr_leave.date_to::DATE) OVERLAPS ('%s', '%s') and
        state='validate'""" % (today, today)
        cr = self._cr
        cr.execute(query)
        leaves_today = cr.fetchall()
        first_day = date.today().replace(day=1)
        last_day = (date.today() + relativedelta(months=1, day=1)) - timedelta(1)
        query = """
        select count(id)
        from hr_leave
        WHERE (hr_leave.date_from::DATE,hr_leave.date_to::DATE) OVERLAPS ('%s', '%s')
        and  state='validate'""" % (first_day, last_day)
        cr = self._cr
        cr.execute(query)
        leaves_this_month = cr.fetchall()

        temp_lst = []
        a = self.env['hr.contract'].search([])
        total_employee = self.env['hr.contract'].search_count([])
        total_salary = 0
        for i in a:
            temp_lst.append(i.wage)
            total_salary += i.wage
        # print(total_salary)
        avg_salary = round((total_salary / total_employee), 2)
        # min_salary = min(temp_lst)
        # max_salary = max(temp_lst)
        # print(min_salary)
        # print(max_salary)
        # print(avg_salary)

        leaves_alloc_req = self.env['hr.leave.allocation'].sudo().search_count(
            [('state', 'in', ['confirm', 'validate1'])])
        timesheet_count = self.env['account.analytic.line'].sudo().search_count(
            [('project_id', '!=', False), ('user_id', '=', uid)])
        timesheet_view_id = self.env.ref('hr_timesheet.hr_timesheet_line_search')
        job_applications = self.env['hr.applicant'].sudo().search_count([])
        all_employee_details = self.env['hr.employee'].sudo().search_count([])
        employee_payroll = self.env['hr.payslip'].sudo().search_count([])

        if employee:
            sql = """select broad_factor from hr_employee_broad_factor where id =%s"""
            self.env.cr.execute(sql, (employee[0]['id'],))
            result = self.env.cr.dictfetchall()
            broad_factor = result[0]['broad_factor']
            if employee[0]['birthday']:
                diff = relativedelta(datetime.today(), employee[0]['birthday'])
                age = diff.years
            else:
                age = False
            if employee[0]['joining_date']:
                diff = relativedelta(datetime.today(), employee[0]['joining_date'])
                years = diff.years
                months = diff.months
                days = diff.days
                experience = '{} years {} months {} days'.format(years, months, days)
            else:
                experience = False
            if employee:
                data = {
                    'broad_factor': broad_factor if broad_factor else 0,
                    'leaves_to_approve': leaves_to_approve,
                    'leaves_today': leaves_today,
                    'leaves_this_month': leaves_this_month,
                    'leaves_alloc_req': leaves_alloc_req,
                    'emp_timesheets': timesheet_count,
                    'job_applications': job_applications,
                    'timesheet_view_id': timesheet_view_id,
                    'experience': experience,
                    'age': age,
                    'employee_payroll': employee_payroll,
                    'all_employee_details': all_employee_details,
                    'avg_salary': avg_salary
                }
                employee[0].update(data)
            return employee
        else:
            return False

    # @api.model
    # def experience_salary_graph(self):
    #     data = []
    #     exp_lst_in_months = []
    #     temp_exp_lst = []
    #     temp_salary_lst = []
    #     for rec in self.env['hr.employee'].sudo().search([]):
    #         temp_exp_lst.append(rec.actual_experience)
    #     for rec in self.env['hr.contract'].sudo().search([]):
    #         temp_salary_lst.append(rec.wage)
    #
    #     for rec in temp_exp_lst:
    #         if rec == '0':
    #             exp_lst_in_months.append(int(rec))
    #         elif ('Years' or 'Year') in rec:
    #             if len(rec) < 10:
    #                 a1 = rec.split()
    #                 exp_lst_in_months.append(int(a1[0]) * 12)
    #             else:
    #                 a2 = rec.split()
    #                 s1 = a2[0]
    #                 s2 = a2[2]
    #                 s3 = (int(s1) * 12) + int(s2)
    #                 exp_lst_in_months.append(s3)
    #         else:
    #             a3 = rec.split()
    #             exp_lst_in_months.append(int(a3[0]))
    #
    #     exp_lst_in_years = []
    #
    #     salary = []
    #     for rec in self.env['hr.employee'].search([]):
    #         emp_salary = rec.contract_id.wage
    #         salary.append(int(emp_salary))
    #     # print(salary)
    #
    #     for rec in exp_lst_in_months:
    #         exp_lst_in_years.append(round((rec / 12), 2))
    #
    #     sum_1 = 0
    #     sum_2 = 0
    #     sum_3 = 0
    #     sum_4 = 0
    #     sum_5 = 0
    #     count_1 = 0
    #     count_2 = 0
    #     count_3 = 0
    #     count_4 = 0
    #     count_5 = 0
    #     for rec in range(self.env['hr.employee'].search_count([])):
    #         if exp_lst_in_years[rec] < 2:
    #             count_1 += 1
    #             sum_1 += salary[rec]
    #         elif exp_lst_in_years[rec] < 4:
    #             count_2 += 1
    #             sum_2 += salary[rec]
    #         elif exp_lst_in_years[rec] < 6:
    #             count_3 += 1
    #             sum_3 += salary[rec]
    #         elif exp_lst_in_years[rec] < 8:
    #             count_4 += 1
    #             sum_4 += salary[rec]
    #         elif exp_lst_in_years[rec] > 8:
    #             count_5 += 1
    #             sum_5 += salary[rec]
    #
    #     avg_1 = round((sum_1 / count_1), 2)
    #     avg_2 = round((sum_2 / count_2), 2)
    #     avg_3 = round((sum_3 / count_3), 2)
    #     avg_4 = round((sum_4 / count_4), 2)
    #     avg_5 = round((sum_5 / count_5), 2)
    #     data.append({'label': 'Below 2 years', 'value': avg_1})
    #     data.append({'label': '2-4 years', 'value': avg_2})
    #     data.append({'label': '4-6 years', 'value': avg_3})
    #     data.append({'label': '6-8 years', 'value': avg_4})
    #     data.append({'label': 'Above 8 years', 'value': avg_5})
    #
    #     # print(data)
    #     return data
    #     # print(avg_1)
    #     # print(avg_2)
    #     # print(avg_3)
    #     # print(avg_4)
    #     # print(avg_5)

    @api.model
    def experience_salary_graph(self):
        data = []
        salary_dict = {
            'Below 2 years': {'count': 0, 'sum': 0},
            '2-4 years': {'count': 0, 'sum': 0},
            '4-6 years': {'count': 0, 'sum': 0},
            '6-8 years': {'count': 0, 'sum': 0},
            'Above 8 years': {'count': 0, 'sum': 0}
        }

        for rec in self.env['hr.employee'].sudo().search([]):
            exp = rec.actual_experience
            salary = rec.contract_id.wage

            if exp == '0':
                exp_in_months = 0
            elif 'Years' in exp or 'Year' in exp:
                exp_parts = exp.split()
                if len(exp_parts) < 10:
                    exp_in_months = int(exp_parts[0]) * 12
                else:
                    exp_in_years = int(exp_parts[0])
                    exp_in_months = exp_in_years * 12 + int(exp_parts[2])
            else:
                exp_parts = exp.split()
                exp_in_months = int(exp_parts[0])

            exp_in_years = exp_in_months / 12
            salary_dict_key = None

            if exp_in_years < 2:
                salary_dict_key = 'Below 2 years'
            elif exp_in_years < 4:
                salary_dict_key = '2-4 years'
            elif exp_in_years < 6:
                salary_dict_key = '4-6 years'
            elif exp_in_years < 8:
                salary_dict_key = '6-8 years'
            else:
                salary_dict_key = 'Above 8 years'

            salary_dict[salary_dict_key]['count'] += 1
            salary_dict[salary_dict_key]['sum'] += int(salary)

        for label, values in salary_dict.items():
            count = values['count']
            total_salary = values['sum']
            average_salary = round(total_salary / count, 2) if count != 0 else 0
            data.append({'label': label, 'value': average_salary})

        return data

    @api.model
    def get_upcoming(self):
        cr = self._cr
        uid = request.session.uid
        employee = self.env['hr.employee'].search([('user_id', '=', uid)], limit=1)

        cr.execute("""select *,
        (to_char(dob,'ddd')::int-to_char(now(),'ddd')::int+total_days)%total_days as dif
        from (select he.id, he.name, to_char(he.birthday, 'Month dd') as birthday,
        hj.name as job_id , he.birthday as dob,
        (to_char((to_char(now(),'yyyy')||'-12-31')::date,'ddd')::int) as total_days
        FROM hr_employee he
        join hr_job hj
        on hj.id = he.job_id
        ) birth
        where (to_char(dob,'ddd')::int-to_char(now(),'DDD')::int+total_days)%total_days between 0 and 15
        order by dif;""")
        birthday = cr.fetchall()
        # e.is_online # was there below
        #        where e.state ='confirm' on line 118/9 #change
        cr.execute("""select e.name, e.date_begin, e.date_end, rc.name as location
        from event_event e
        left join res_partner rp
        on e.address_id = rp.id
        left join res_country rc
        on rc.id = rp.country_id
        and (e.date_begin >= now()
        and e.date_begin <= now() + interval '15 day')
        or (e.date_end >= now()
        and e.date_end <= now() + interval '15 day')
        order by e.date_begin """)
        event = cr.fetchall()
        announcement = []
        if employee:
            department = employee.department_id
            job_id = employee.job_id
            sql = """select ha.name, ha.announcement_reason
            from hr_announcement ha
            left join hr_employee_announcements hea
            on hea.announcement = ha.id
            left join hr_department_announcements hda
            on hda.announcement = ha.id
            left join hr_job_position_announcements hpa
            on hpa.announcement = ha.id
            where ha.state = 'approved' and
            ha.date_start <= now()::date and
            ha.date_end >= now()::date and
            (ha.is_announcement = True or
            (ha.is_announcement = False
            and ha.announcement_type = 'employee'
            and hea.employee = %s)""" % employee.id
            if department:
                sql += """ or
                (ha.is_announcement = False and
                ha.announcement_type = 'department'
                and hda.department = %s)""" % department.id
            if job_id:
                sql += """ or
                (ha.is_announcement = False and
                ha.announcement_type = 'job_position'
                and hpa.job_position = %s)""" % job_id.id
            sql += ')'
            cr.execute(sql)
            announcement = cr.fetchall()
        return {
            'birthday': birthday,
            'event': event,
            'announcement': announcement
        }

    @api.model
    def get_dept_employee(self):
        cr = self._cr
        cr.execute("""select department_id, hr_department.name,count(*)
from hr_employee join hr_department on hr_department.id=hr_employee.department_id
group by hr_employee.department_id,hr_department.name""")
        dat = cr.fetchall()
        data = []
        for i in range(0, len(dat)):
            data.append({'label': dat[i][1], 'value': dat[i][2]})
        return data

    @api.model
    def get_department_leave(self):
        month_list = []
        graph_result = []
        for i in range(5, -1, -1):
            last_month = datetime.now() - relativedelta(months=i)
            text = format(last_month, '%B %Y')
            month_list.append(text)
        self.env.cr.execute("""select id, name from hr_department where active=True """)
        departments = self.env.cr.dictfetchall()
        department_list = [x['name'] for x in departments]
        for month in month_list:
            leave = {}
            for dept in departments:
                leave[dept['name']] = 0
            vals = {
                'l_month': month,
                'leave': leave
            }
            graph_result.append(vals)
        sql = """
        SELECT h.id, h.employee_id,h.department_id
             , extract('month' FROM y)::int AS leave_month
             , to_char(y, 'Month YYYY') as month_year
             , GREATEST(y                    , h.date_from) AS date_from
             , LEAST   (y + interval '1 month', h.date_to)   AS date_to
        FROM  (select * from hr_leave where state = 'validate') h
             , generate_series(date_trunc('month', date_from::timestamp)
                             , date_trunc('month', date_to::timestamp)
                             , interval '1 month') y
        where date_trunc('month', GREATEST(y , h.date_from)) >= date_trunc('month', now()) - interval '6 month' and
        date_trunc('month', GREATEST(y , h.date_from)) <= date_trunc('month', now())
        and h.department_id is not null
        """
        self.env.cr.execute(sql)
        results = self.env.cr.dictfetchall()
        leave_lines = []
        for line in results:
            employee = self.browse(line['employee_id'])
            from_dt = fields.Datetime.from_string(line['date_from'])
            to_dt = fields.Datetime.from_string(line['date_to'])
            days = employee.get_work_days_dashboard(from_dt, to_dt)
            line['days'] = days
            vals = {
                'department': line['department_id'],
                'l_month': line['month_year'],
                'days': days
            }
            leave_lines.append(vals)
        if leave_lines:
            df = pd.DataFrame(leave_lines)
            rf = df.groupby(['l_month', 'department']).sum()
            result_lines = rf.to_dict('index')
            for month in month_list:
                for line in result_lines:
                    if month.replace(' ', '') == line[0].replace(' ', ''):
                        match = list(filter(lambda d: d['l_month'] in [month], graph_result))[0]['leave']
                        dept_name = self.env['hr.department'].browse(line[1]).name
                        if match:
                            match[dept_name] = result_lines[line]['days']
        for result in graph_result:
            result['l_month'] = result['l_month'].split(' ')[:1][0].strip()[:3] + " " + \
                                result['l_month'].split(' ')[1:2][0]

        return graph_result, department_list

    def get_work_days_dashboard(self, from_datetime, to_datetime, compute_leaves=False, calendar=None, domain=None):
        resource = self.resource_id
        calendar = calendar or self.resource_calendar_id

        if not from_datetime.tzinfo:
            from_datetime = from_datetime.replace(tzinfo=utc)
        if not to_datetime.tzinfo:
            to_datetime = to_datetime.replace(tzinfo=utc)
        from_full = from_datetime - timedelta(days=1)
        to_full = to_datetime + timedelta(days=1)
        intervals = calendar._attendance_intervals_batch(from_full, to_full, resource)
        day_total = defaultdict(float)
        for start, stop, meta in intervals[resource.id]:
            day_total[start.date()] += (stop - start).total_seconds() / 3600
        if compute_leaves:
            intervals = calendar._work_intervals_batch(from_datetime, to_datetime, resource, domain)
        else:
            intervals = calendar._attendance_intervals_batch(from_datetime, to_datetime, resource)
        day_hours = defaultdict(float)
        for start, stop, meta in intervals[resource.id]:
            day_hours[start.date()] += (stop - start).total_seconds() / 3600
        days = sum(
            float_utils.round(ROUNDING_FACTOR * day_hours[day] / day_total[day]) / ROUNDING_FACTOR
            for day in day_hours
        )
        return days

    @api.model
    def employee_leave_trend(self):
        leave_lines = []
        month_list = []
        graph_result = []
        for i in range(5, -1, -1):
            last_month = datetime.now() - relativedelta(months=i)
            text = format(last_month, '%B %Y')
            month_list.append(text)
        uid = request.session.uid
        employee = self.env['hr.employee'].sudo().search_read([('user_id', '=', uid)], limit=1)
        for month in month_list:
            vals = {
                'l_month': month,
                'leave': 0
            }
            graph_result.append(vals)
        sql = """
                SELECT h.id, h.employee_id
                     , extract('month' FROM y)::int AS leave_month
                     , to_char(y, 'Month YYYY') as month_year
                     , GREATEST(y                    , h.date_from) AS date_from
                     , LEAST   (y + interval '1 month', h.date_to)   AS date_to
                FROM  (select * from hr_leave where state = 'validate') h
                     , generate_series(date_trunc('month', date_from::timestamp)
                                     , date_trunc('month', date_to::timestamp)
                                     , interval '1 month') y
                where date_trunc('month', GREATEST(y , h.date_from)) >= date_trunc('month', now()) - interval '6 month' and
                date_trunc('month', GREATEST(y , h.date_from)) <= date_trunc('month', now())
                and h.employee_id = %s
                """
        self.env.cr.execute(sql, (employee[0]['id'],))
        results = self.env.cr.dictfetchall()
        for line in results:
            employee = self.browse(line['employee_id'])
            from_dt = fields.Datetime.from_string(line['date_from'])
            to_dt = fields.Datetime.from_string(line['date_to'])
            days = employee.get_work_days_dashboard(from_dt, to_dt)
            line['days'] = days
            vals = {
                'l_month': line['month_year'],
                'days': days
            }
            leave_lines.append(vals)
        if leave_lines:
            df = pd.DataFrame(leave_lines)
            rf = df.groupby(['l_month']).sum()
            result_lines = rf.to_dict('index')
            for line in result_lines:
                match = list(filter(lambda d: d['l_month'].replace(' ', '') == line.replace(' ', ''), graph_result))
                if match:
                    match[0]['leave'] = result_lines[line]['days']
        for result in graph_result:
            result['l_month'] = result['l_month'].split(' ')[:1][0].strip()[:3] + " " + \
                                result['l_month'].split(' ')[1:2][0]
        return graph_result

    @api.model
    def join_resign_trends(self):
        cr = self._cr
        month_list = []
        join_trend = []
        resign_trend = []
        for i in range(11, -1, -1):
            last_month = datetime.now() - relativedelta(months=i)
            text = format(last_month, '%B %Y')
            month_list.append(text)
        for month in month_list:
            vals = {
                'l_month': month,
                'count': 0
            }
            join_trend.append(vals)
        for month in month_list:
            vals = {
                'l_month': month,
                'count': 0
            }
            resign_trend.append(vals)
        cr.execute('''select to_char(joining_date, 'Month YYYY') as l_month, count(id) from hr_employee
        WHERE joining_date BETWEEN CURRENT_DATE - INTERVAL '12 months'
        AND CURRENT_DATE + interval '1 month - 1 day'
        group by l_month''')
        join_data = cr.fetchall()
        cr.execute('''select to_char(resign_date, 'Month YYYY') as l_month, count(id) from hr_employee
        WHERE resign_date BETWEEN CURRENT_DATE - INTERVAL '12 months'
        AND CURRENT_DATE + interval '1 month - 1 day'
        group by l_month;''')
        resign_data = cr.fetchall()

        for line in join_data:
            match = list(filter(lambda d: d['l_month'].replace(' ', '') == line[0].replace(' ', ''), join_trend))
            if match:
                match[0]['count'] = line[1]
        for line in resign_data:
            match = list(filter(lambda d: d['l_month'].replace(' ', '') == line[0].replace(' ', ''), resign_trend))
            if match:
                match[0]['count'] = line[1]
        for join in join_trend:
            join['l_month'] = join['l_month'].split(' ')[:1][0].strip()[:3]
        for resign in resign_trend:
            resign['l_month'] = resign['l_month'].split(' ')[:1][0].strip()[:3]
        graph_result = [{
            'name': 'Join',
            'values': join_trend
        }, {
            'name': 'Resign',
            'values': resign_trend
        }]

        return graph_result

    @api.model
    def get_attrition_rate(self):

        month_attrition = []
        monthly_join_resign = self.join_resign_trends()
        month_join = monthly_join_resign[0]['values']
        month_resign = monthly_join_resign[1]['values']
        sql = """
        SELECT (date_trunc('month', CURRENT_DATE))::date - interval '1' month * s.a AS month_start
        FROM generate_series(0,11,1) AS s(a);"""
        self._cr.execute(sql)
        month_start_list = self._cr.fetchall()
        for month_date in month_start_list:
            self._cr.execute("""select count(id), to_char(date '%s', 'Month YYYY') as l_month from hr_employee
            where resign_date> date '%s' or resign_date is null and joining_date < date '%s'
            """ % (month_date[0], month_date[0], month_date[0],))
            month_emp = self._cr.fetchone()
            # month_emp = (month_emp[0], month_emp[1].split(' ')[:1][0].strip()[:3])
            match_join = \
                list(filter(lambda d: d['l_month'] == month_emp[1].split(' ')[:1][0].strip()[:3], month_join))[0][
                    'count']
            match_resign = \
                list(filter(lambda d: d['l_month'] == month_emp[1].split(' ')[:1][0].strip()[:3], month_resign))[0][
                    'count']
            month_avg = (month_emp[0] + match_join - match_resign + month_emp[0]) / 2
            attrition_rate = (match_resign / month_avg) * 100 if month_avg != 0 else 0
            vals = {
                # 'month': month_emp[1].split(' ')[:1][0].strip()[:3] + ' ' + month_emp[1].split(' ')[-1:][0],
                'month': month_emp[1].split(' ')[:1][0].strip()[:3],
                'attrition_rate': round(float(attrition_rate), 2)
            }
            month_attrition.append(vals)

        return month_attrition


class BroadFactor(models.Model):
    _inherit = 'hr.leave.type'

    emp_broad_factor = fields.Boolean(string="Broad Factor", help="If check it will display in broad factor type")
