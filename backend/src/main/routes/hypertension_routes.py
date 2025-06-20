# pylint: disable=W0613
import io

from flask import Blueprint, Response, jsonify, request
from src.errors.error_handler import handle_errors
from src.main.adapters.request_adapter import request_adapter
from src.main.composers.hypertension_dashboard_composer import (
    hypertension_dashboard_get_age_group_gender,
    hypertension_dashboard_get_age_groups_location,
    hypertension_dashboard_get_complications,
    hypertension_dashboard_get_exams_count,
    hypertension_dashboard_get_imc,
    hypertension_dashboard_get_individual_exams_count,
    hypertension_dashboard_get_nominal_list,
    hypertension_dashboard_get_nominal_list_download,
    hypertension_dashboard_get_professionals_count,
    hypertension_dashboard_get_total,
)
from src.main.server.cache import cache
from src.presentations.validators.base_validation import _validation
from src.presentations.validators.schema.nominal_list import schema

hypertension_bp = Blueprint("hypertension", __name__)


class HypertensionPath:
    root_path = '/v1/arterial-hypertension'
    urls = {
        'total': '/total',
        'age_group_gender': '/age-group-gender',
        'age_group_location': '/age-group-location',
        'imc': '/imc',
        'complications': '/complications',
        'exams': '/exams',
        'professionals': '/professionals',
        'get_hypertensive_list': '/get-hypertensive-list',
        'get_nominal_list': '/get-nominal-list'

    }


hypertension = HypertensionPath()
urls = hypertension.urls


@hypertension_bp.route(f"{urls['total']}", methods=['GET'],
                       endpoint='total')
@hypertension_bp.route(f"{urls['total']}/<cnes>", methods=['GET'],
                       endpoint='total_id')
def get_total(cnes=None):
    http_response = None
    response = None
    try:
        http_response = request_adapter(
            request, hypertension_dashboard_get_total())
        response = jsonify(http_response.body)
    except Exception as exception:
        http_response = handle_errors(exception)
        response = jsonify(http_response.body)

    return response, http_response.status_code


@hypertension_bp.route(f"{urls['age_group_gender']}", methods=['GET'],
                       endpoint='get_age_group_gender')
@hypertension_bp.route(f"{urls['age_group_gender']}/<cnes>", methods=['GET'],
                       endpoint='get_age_group_gender_id')
def get_age_group_gender(cnes=None):
    http_response = None
    response = None
    try:
        http_response = request_adapter(
            request, hypertension_dashboard_get_age_group_gender())
        response = jsonify(http_response.body)
    except Exception as exception:
        http_response = handle_errors(exception)
        response = jsonify(http_response.body)

    return response, http_response.status_code


@hypertension_bp.route(f"{urls['age_group_location']}", methods=['GET'],
                       endpoint='get_age_group_location')
@hypertension_bp.route(f"{urls['age_group_location']}/<cnes>", methods=['GET'],
                       endpoint='get_age_group_location_id')
def get_age_group_location(cnes=None):
    http_response = None
    response = None
    try:
        http_response = request_adapter(
            request, hypertension_dashboard_get_age_groups_location())
        response = jsonify(http_response.body)
    except Exception as exception:
        http_response = handle_errors(exception)
        response = jsonify(http_response.body)

    return response, http_response.status_code


@hypertension_bp.route(f"{urls['imc']}", methods=['GET'],
                       endpoint='get_imc')
@hypertension_bp.route(f"{urls['imc']}/<int:cnes>", methods=['GET'],
                       endpoint='get_imc_id')
def get_imc(cnes=None):
    if cnes:
        request.view_args['cnes'] = int(request.view_args['cnes'])
    http_response = None
    response = None
    try:
        http_response = request_adapter(
            request, hypertension_dashboard_get_imc())
        response = jsonify(http_response.body)
    except Exception as exception:
        http_response = handle_errors(exception)
        response = jsonify(http_response.body)

    return response, http_response.status_code


@hypertension_bp.route(f"{urls['complications']}", methods=['GET'],
                       endpoint='get_complications')
@hypertension_bp.route(f"{urls['complications']}/<cnes>", methods=['GET'],
                       endpoint='get_complications_id')
def get_complications(cnes=None):
    http_response = None
    response = None
    try:
        http_response = request_adapter(
            request, hypertension_dashboard_get_complications())
        response = jsonify(http_response.body)
    except Exception as exception:
        http_response = handle_errors(exception)
        response = jsonify(http_response.body)

    return response, http_response.status_code


@hypertension_bp.route(f"{urls['exams']}", methods=['GET'],
                       endpoint='get_exams')
@hypertension_bp.route(f"{urls['exams']}/<cnes>", methods=['GET'],
                       endpoint='get_exams_id')
def get_exams(cnes=None):
    http_response = None
    response = None
    try:
        http_response = request_adapter(
            request, hypertension_dashboard_get_exams_count())
        response = jsonify(http_response.body)
    except Exception as exception:
        http_response = handle_errors(exception)
        response = jsonify(http_response.body)

    return response, http_response.status_code


@hypertension_bp.route(f"{urls['professionals']}", methods=['GET'],
                       endpoint='get_professionals')
@hypertension_bp.route(f"{urls['professionals']}/<cnes>", methods=['GET'],
                       endpoint='get_professionals_id')
def get_professionals(cnes=None):
    http_response = None
    response = None
    try:
        http_response = request_adapter(
            request, hypertension_dashboard_get_professionals_count())
        response = jsonify(http_response.body)
    except Exception as exception:
        http_response = handle_errors(exception)
        response = jsonify(http_response.body)

    return response, http_response.status_code


@hypertension_bp.route(f"{urls['get_hypertensive_list']}", methods=['GET'],
                       endpoint='get_hypertenses_list')
@hypertension_bp.route(f"{urls['get_hypertensive_list']}/<cnes>", methods=['GET'],
                       endpoint='get_hypertenses_list_id')
def get_hypertenses_list(cnes=None):
    if cnes:
        request.view_args['cnes'] = int(request.view_args['cnes'])

    http_response = None
    response = None
    try:
        http_response = request_adapter(
            request, hypertension_dashboard_get_individual_exams_count())
        response = jsonify(http_response.body)
    except Exception as exception:
        http_response = handle_errors(exception)
        response = jsonify(http_response.body)

    return response, http_response.status_code


@hypertension_bp.route(f"{urls['get_nominal_list']}/<cnes>", methods=['GET'],
                       endpoint='get_nominal_list_id')
def get_nominal_list(cnes=None):
    if cnes:
        request.view_args['cnes'] = int(request.view_args['cnes'])

    http_response = None
    response = None

    try:
        _validation(request.args.to_dict(), schema)
        http_response = request_adapter(
            request, hypertension_dashboard_get_nominal_list())
        response = jsonify(http_response.body)

    except Exception as exception:
        http_response = handle_errors(exception)
        response = jsonify(http_response.body)

    return response, http_response.body


@hypertension_bp.route(f"{urls['get_nominal_list']}/download/<cnes>", methods=['GET'],
                       endpoint='get_nominal_list_id_download')
def get_nominal_list_download(cnes=None):
    if cnes:
        request.view_args['cnes'] = int(request.view_args['cnes'])

    http_response = None
    response = None

    try:
        _validation(request.args.to_dict(), schema)
        response = request_adapter(
            request, hypertension_dashboard_get_nominal_list_download()
        )

        buffer = io.BytesIO()
        response.to_excel(buffer)

        headers = {
            'Content-Disposition': 'attachment; filename=lista_nominal.xlsx',
            'Content-type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
        return Response(buffer.getvalue(), mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers=headers,)

    except Exception as exception:
        http_response = handle_errors(exception)
        response = jsonify(http_response.body)

    return response, 200
