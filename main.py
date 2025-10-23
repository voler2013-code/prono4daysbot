import asyncio
import aiohttp
import json
import math
import statistics
from datetime import datetime, timedelta
import re
import os
from typing import List, Dict, Optional, Tuple
import logging
from aiohttp import web
import time

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Token del bot de Telegram (obtener de @BotFather)
BOT_TOKEN = os.getenv('BOT_TOKEN', 'YOUR_BOT_TOKEN_HERE')

# URLs de los modelos meteorológicos
WEATHER_MODELS = {
    'icon_seamless': 'https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m,relative_humidity_2m,cloud_cover,wind_speed_10m,wind_direction_10m,temperature_850hPa,temperature_800hPa,relative_humidity_850hPa,relative_humidity_800hPa,wind_speed_850hPa,wind_speed_800hPa,wind_direction_850hPa,wind_direction_800hPa&models=icon_seamless&timezone=America%2FSao_Paulo&format=json',
    'gfs_seamless': 'https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m,relative_humidity_2m,cloud_cover,wind_speed_10m,wind_direction_10m,temperature_850hPa,temperature_800hPa,temperature_750hPa,relative_humidity_850hPa,relative_humidity_800hPa,relative_humidity_750hPa,wind_speed_850hPa,wind_speed_800hPa,wind_speed_750hPa,wind_direction_850hPa,wind_direction_800hPa,wind_direction_750hPa&models=gfs_seamless&format=json',
    'meteofrance_seamless': 'https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m,relative_humidity_2m,cloud_cover,wind_speed_10m,wind_direction_10m,temperature_850hPa,temperature_800hPa,temperature_750hPa,relative_humidity_850hPa,relative_humidity_800hPa,relative_humidity_750hPa,wind_speed_850hPa,wind_speed_800hPa,wind_speed_750hPa,wind_direction_850hPa,wind_direction_800hPa,wind_direction_750hPa&models=meteofrance_seamless&timezone=America%2FSao_Paulo&format=json',
    'ecmwf_ifs': 'https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m,relative_humidity_2m,cloud_cover,wind_speed_10m,wind_direction_10m,temperature_850hPa,relative_humidity_850hPa,wind_speed_850hPa,wind_direction_850hPa&models=ecmwf_ifs&timezone=America%2FSao_Paulo&forecast_days=7&format=json',
    'ukmo_seamless': 'https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m,relative_humidity_2m,cloud_cover,wind_speed_10m,wind_direction_10m,temperature_850hPa,temperature_800hPa,temperature_750hPa,relative_humidity_850hPa,relative_humidity_800hPa,relative_humidity_750hPa,wind_speed_850hPa,wind_speed_800hPa,wind_speed_750hPa,wind_direction_850hPa,wind_direction_800hPa,wind_direction_750hPa&models=ukmo_seamless&timezone=America%2FSao_Paulo&format=json',
    'gem_seamless': 'https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m,relative_humidity_2m,cloud_cover,wind_speed_10m,wind_direction_10m,temperature_850hPa,temperature_800hPa,temperature_750hPa,relative_humidity_850hPa,relative_humidity_800hPa,relative_humidity_750hPa,wind_speed_850hPa,wind_speed_800hPa,wind_speed_750hPa,wind_direction_850hPa,wind_direction_800hPa,wind_direction_750hPa&timezone=America%2FSao_Paulo&models=gem_seamless&format=json',
    'cma_grapes_global': 'https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m,relative_humidity_2m,cloud_cover,wind_speed_10m,wind_direction_10m,temperature_850hPa,temperature_800hPa,temperature_750hPa,relative_humidity_850hPa,relative_humidity_800hPa,relative_humidity_750hPa,wind_speed_850hPa,wind_speed_800hPa,wind_speed_750hPa,wind_direction_850hPa,wind_direction_800hPa,wind_direction_750hPa&timezone=America%2FSao_Paulo&models=cma_grapes_global&format=json'
}

# Ubicaciones predefinidas
LOCATIONS = {
    'S.Jor': (-31.32, -64.34),
    'Cuchi': (-30.99, -64.71),
    'V.Alp': (-32.02, -64.81),
    'Peder': (-31.76, -64.65),
    'N.Pau': (-31.72, -65.00),
    'Merlo': (-32.34, -64.98)
}

# Direcciones de viento
WIND_DIRECTIONS = [
    ('N', 348.75, 360), ('N', 0, 11.25),
    ('NNE', 11.25, 33.75), ('NE', 33.75, 56.25),
    ('ENE', 56.25, 78.75), ('E', 78.75, 101.25),
    ('ESE', 101.25, 123.75), ('SE', 123.75, 146.25),
    ('SSE', 146.25, 168.75), ('S', 168.75, 191.25),
    ('SSO', 191.25, 213.75), ('SO', 213.75, 236.25),
    ('OSO', 236.25, 258.75), ('O', 258.75, 281.25),
    ('ONO', 281.25, 303.75), ('NO', 303.75, 326.25),
    ('NNO', 326.25, 348.75)
]

DAYS_ES = ['lunes', 'martes', 'miércoles', 'jueves', 'viernes', 'sábado', 'domingo']
MONTHS_ES = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 
             'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']

class WeatherBot:
    def __init__(self):
        self.session = None
        self.last_update_id = 0
        
    async def init_session(self):
        """Inicializar sesión HTTP"""
        if not self.session:
            self.session = aiohttp.ClientSession()
    
    async def close_session(self):
        """Cerrar sesión HTTP"""
        if self.session:
            await self.session.close()
    
    def degrees_to_direction(self, degrees: float) -> str:
        """Convertir grados a dirección cardinal"""
        if degrees is None:
            return "?"
        
        for direction, min_deg, max_deg in WIND_DIRECTIONS:
            if min_deg <= degrees < max_deg:
                return direction
        return "N"
    
    def calculate_dew_point(self, temp: float, humidity: float) -> float:
        """Calcular punto de rocío"""
        if temp is None or humidity is None or humidity <= 0:
            return None
        try:
            return temp + (35 * math.log10(humidity / 100))
        except (ValueError, ZeroDivisionError):
            return None
    
    def calculate_thermal_velocity(self, rocio_termica: float, rocio: float, temp: float) -> float:
        """Calcular velocidad térmica"""
        if any(x is None for x in [rocio_termica, rocio, temp]):
            return None
        
        try:
            if abs(temp - rocio) == 0:
                return None
            
            numerator = (1.1 ** abs(rocio_termica - rocio)) - 1
            denominator = 1.1 ** abs(temp - rocio)
            
            if denominator == 0 or numerator < 0:
                return None
                
            result = 5.6 * math.sqrt(numerator / denominator)
            return result
        except (ValueError, ZeroDivisionError, OverflowError):
            return None
    
    async def fetch_weather_data(self, lat: float, lon: float) -> Dict:
        """Obtener datos meteorológicos de todos los modelos"""
        all_data = {}
        
        for model_name, url_template in WEATHER_MODELS.items():
            try:
                url = url_template.format(lat=lat, lon=lon)
                
                async with self.session.get(url, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        all_data[model_name] = data
                        logger.info(f"Datos obtenidos exitosamente del modelo {model_name}")
                    elif response.status == 429:
                        logger.warning(f"Error 429 para modelo {model_name}")
                        await asyncio.sleep(2)  # Esperar antes del siguiente request
                    else:
                        logger.error(f"Error {response.status} para modelo {model_name}")
                        
            except asyncio.TimeoutError:
                logger.error(f"Timeout para modelo {model_name}")
            except Exception as e:
                logger.error(f"Error obteniendo datos del modelo {model_name}: {e}")
            
            # Pequeña pausa entre requests
            await asyncio.sleep(0.5)
        
        return all_data
    
    def process_weather_data(self, all_data: Dict, day_index: int) -> Dict:
        """Procesar datos meteorológicos para un día específico"""
        processed_data = {}
        
        # Buscar el índice correspondiente a las 15:00 del día especificado
        target_hour_index = None
        
        for model_name, data in all_data.items():
            if 'hourly' in data and 'time' in data['hourly']:
                times = data['hourly']['time']
                for i, time_str in enumerate(times):
                    dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                    if dt.day == (datetime.now() + timedelta(days=day_index)).day and dt.hour == 15:
                        target_hour_index = i
                        break
                if target_hour_index is not None:
                    break
        
        if target_hour_index is None:
            # Usar índice aproximado si no se encuentra exactamente
            target_hour_index = day_index * 24 + 15
        
        # Recopilar todos los valores para cada variable
        variables = {
            'wind_speed_10m': [],
            'wind_speed_850hPa': [],
            'wind_speed_800hPa': [],
            'wind_speed_750hPa': [],
            'wind_direction_10m': [],
            'wind_direction_850hPa': [],
            'wind_direction_800hPa': [],
            'wind_direction_750hPa': [],
            'cloud_cover': [],
            'temperature_2m': [],
            'temperature_850hPa': [],
            'temperature_800hPa': [],
            'temperature_750hPa': [],
            'relative_humidity_2m': [],
            'relative_humidity_850hPa': [],
            'relative_humidity_800hPa': [],
            'relative_humidity_750hPa': []
        }
        
        for model_name, data in all_data.items():
            if 'hourly' not in data:
                continue
                
            hourly = data['hourly']
            
            for var_name in variables.keys():
                if var_name in hourly and len(hourly[var_name]) > target_hour_index:
                    value = hourly[var_name][target_hour_index]
                    if value is not None:
                        variables[var_name].append(float(value))
        
        # Calcular promedios
        for var_name, values in variables.items():
            if values:
                processed_data[f"{var_name}_avg"] = statistics.mean(values)
                if len(values) > 1:
                    processed_data[f"{var_name}_std"] = statistics.stdev(values)
                else:
                    processed_data[f"{var_name}_std"] = 0
            else:
                processed_data[f"{var_name}_avg"] = None
                processed_data[f"{var_name}_std"] = None
        
        # Calcular puntos de rocío y velocidades térmicas
        levels = ['2m', '850hPa', '800hPa', '750hPa']
        dew_points = {}
        
        for level in levels:
            temp_key = f'temperature_{level}_avg'
            humidity_key = f'relative_humidity_{level}_avg'
            
            if processed_data.get(temp_key) is not None and processed_data.get(humidity_key) is not None:
                dew_points[level] = self.calculate_dew_point(
                    processed_data[temp_key], 
                    processed_data[humidity_key]
                )
        
        # Calcular velocidades térmicas para cada nivel
        rocio_termica = dew_points.get('2m')
        
        for level in ['850hPa', '800hPa', '750hPa']:
            if rocio_termica is not None and level in dew_points and dew_points[level] is not None:
                temp = processed_data.get(f'temperature_{level}_avg')
                if temp is not None:
                    thermal_vel = self.calculate_thermal_velocity(
                        rocio_termica, dew_points[level], temp
                    )
                    processed_data[f'thermal_velocity_{level}'] = thermal_vel
        
        return processed_data
    
    def format_table_for_day(self, day_index: int, locations_data: Dict) -> str:
        """Formatear tabla para un día específico"""
        # Obtener fecha
        target_date = datetime.now() + timedelta(days=day_index)
        day_name = DAYS_ES[target_date.weekday()]
        month_name = MONTHS_ES[target_date.month - 1]
        
        if day_index == 0:
            day_prefix = "hoy"
        elif day_index == 1:
            day_prefix = "mañ"
        else:
            day_prefix = f"pas+{day_index-1}"
        
        header = f"Prono: {day_prefix} {day_name} {target_date.day} {month_name} 15hs"
        
        table = f"{header}\n"
        table += "*****|viento km/h      |nb|térmica m/s\n"
        table += "*****| ↓ |1,5k|2k |2,5k|% |1,5k|2k  |2,5k|\n"
        table += "\n"
        
        for location_name in LOCATIONS.keys():
            if location_name not in locations_data:
                continue
                
            data = locations_data[location_name]
            
            # Primera fila de la ubicación
            wind_10m = round(data.get('wind_speed_10m_avg', 0)) if data.get('wind_speed_10m_avg') is not None else 0
            wind_850 = round(data.get('wind_speed_850hPa_avg', 0)) if data.get('wind_speed_850hPa_avg') is not None else 0
            wind_800 = round(data.get('wind_speed_800hPa_avg', 0)) if data.get('wind_speed_800hPa_avg') is not None else 0
            wind_750 = round(data.get('wind_speed_750hPa_avg', 0)) if data.get('wind_speed_750hPa_avg') is not None else 0
            cloud_cover = round(data.get('cloud_cover_avg', 0)) if data.get('cloud_cover_avg') is not None else 0
            
            thermal_850 = data.get('thermal_velocity_850hPa')
            thermal_800 = data.get('thermal_velocity_800hPa')
            thermal_750 = data.get('thermal_velocity_750hPa')
            
            thermal_850_str = f"{thermal_850:.1f}" if thermal_850 is not None else "0.0"
            thermal_800_str = f"{thermal_800:.1f}" if thermal_800 is not None else "0.0"
            thermal_750_str = f"{thermal_750:.1f}" if thermal_750 is not None else "0.0"
            
            row1 = f"{location_name}|{wind_10m:2} |{wind_850:3} |{wind_800:2} |{wind_750:3} |{cloud_cover:2}|{thermal_850_str:>4}|{thermal_800_str:>4}|{thermal_750_str:>4}|"
            
            # Segunda fila de la ubicación (direcciones y desviaciones)
            dir_10m = self.degrees_to_direction(data.get('wind_direction_10m_avg'))
            dir_850 = self.degrees_to_direction(data.get('wind_direction_850hPa_avg'))
            dir_800 = self.degrees_to_direction(data.get('wind_direction_800hPa_avg'))
            dir_750 = self.degrees_to_direction(data.get('wind_direction_750hPa_avg'))
            
            # Desviaciones estándar de velocidades térmicas (simuladas por ahora)
            std_850 = abs(thermal_850 * 0.1) if thermal_850 is not None else 0.1
            std_800 = abs(thermal_800 * 0.1) if thermal_800 is not None else 0.1
            std_750 = abs(thermal_750 * 0.1) if thermal_750 is not None else 0.1
            
            row2 = f"{location_name}|{dir_10m:>3}|{dir_850:>4}|{dir_800:>3}|{dir_750:>4}|% |±{std_850:.1f}|±{std_800:.1f}|±{std_750:.1f}|"
            
            table += row1 + "\n"
            table += row2 + "\n"
            table += "\n"
        
        return table
    
    def format_custom_location_table(self, day_index: int, lat: float, lon: float, data: Dict) -> str:
        """Formatear tabla para ubicación personalizada"""
        target_date = datetime.now() + timedelta(days=day_index)
        day_name = DAYS_ES[target_date.weekday()]
        month_name = MONTHS_ES[target_date.month - 1]
        
        if day_index == 0:
            day_prefix = "hoy"
        elif day_index == 1:
            day_prefix = "mañ"
        else:
            day_prefix = f"pas+{day_index-1}"
        
        header = f"Prono: {day_prefix} {day_name} {target_date.day} {month_name} 15hs"
        
        table = f"{header}\n"
        table += "*****|viento km/h      |nb|térmica m/s\n"
        table += "*****| ↓ |1,5k|2k |2,5k|% |1,5k|2k  |2,5k|\n"
        table += "\n"
        
        # Formatear coordenadas
        lat_str = f"{lat:.1f}"
        lon_str = f"{lon:.1f}"
        
        # Primera fila
        wind_10m = round(data.get('wind_speed_10m_avg', 0)) if data.get('wind_speed_10m_avg') is not None else 0
        wind_850 = round(data.get('wind_speed_850hPa_avg', 0)) if data.get('wind_speed_850hPa_avg') is not None else 0
        wind_800 = round(data.get('wind_speed_800hPa_avg', 0)) if data.get('wind_speed_800hPa_avg') is not None else 0
        wind_750 = round(data.get('wind_speed_750hPa_avg', 0)) if data.get('wind_speed_750hPa_avg') is not None else 0
        cloud_cover = round(data.get('cloud_cover_avg', 0)) if data.get('cloud_cover_avg') is not None else 0
        
        thermal_850 = data.get('thermal_velocity_850hPa')
        thermal_800 = data.get('thermal_velocity_800hPa')
        thermal_750 = data.get('thermal_velocity_750hPa')
        
        thermal_850_str = f"{thermal_850:.1f}" if thermal_850 is not None else "0.0"
        thermal_800_str = f"{thermal_800:.1f}" if thermal_800 is not None else "0.0"
        thermal_750_str = f"{thermal_750:.1f}" if thermal_750 is not None else "0.0"
        
        row1 = f"{lat_str:>5}|{wind_10m:2} |{wind_850:3} |{wind_800:2} |{wind_750:3} |{cloud_cover:2}|{thermal_850_str:>4}|{thermal_800_str:>4}|{thermal_750_str:>4}|"
        
        # Segunda fila
        dir_10m = self.degrees_to_direction(data.get('wind_direction_10m_avg'))
        dir_850 = self.degrees_to_direction(data.get('wind_direction_850hPa_avg'))
        dir_800 = self.degrees_to_direction(data.get('wind_direction_800hPa_avg'))
        dir_750 = self.degrees_to_direction(data.get('wind_direction_750hPa_avg'))
        
        std_850 = abs(thermal_850 * 0.1) if thermal_850 is not None else 0.1
        std_800 = abs(thermal_800 * 0.1) if thermal_800 is not None else 0.1
        std_750 = abs(thermal_750 * 0.1) if thermal_750 is not None else 0.1
        
        row2 = f"{lon_str:>5}|{dir_10m:>3}|{dir_850:>4}|{dir_800:>3}|{dir_750:>4}|% |±{std_850:.1f}|±{std_800:.1f}|±{std_750:.1f}|"
        
        table += row1 + "\n"
        table += row2 + "\n"
        
        return table
    
    def parse_coordinates(self, text: str) -> Optional[Tuple[float, float]]:
        """Parsear coordenadas del texto"""
        pattern = r'(-?\d+\.?\d*),\s*(-?\d+\.?\d*)'
        match = re.search(pattern, text)
        
        if match:
            try:
                lat = float(match.group(1))
                lon = float(match.group(2))
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    return lat, lon
            except ValueError:
                pass
        
        return None
    
    async def send_message(self, chat_id: int, text: str):
        """Enviar mensaje a Telegram"""
        url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
        data = {
            'chat_id': chat_id,
            'text': text,
            'parse_mode': 'HTML'
        }
        
        try:
            async with self.session.post(url, json=data) as response:
                if response.status != 200:
                    logger.error(f"Error enviando mensaje: {response.status}")
        except Exception as e:
            logger.error(f"Error enviando mensaje: {e}")
    
    async def process_message(self, message: Dict):
        """Procesar mensaje recibido"""
        try:
            chat_id = message['chat']['id']
            text = message.get('text', '').strip()
            
            # Verificar si son coordenadas personalizadas
            coordinates = self.parse_coordinates(text)
            
            if coordinates:
                lat, lon = coordinates
                logger.info(f"Procesando coordenadas personalizadas: {lat}, {lon}")
                
                # Obtener datos meteorológicos
                weather_data = await self.fetch_weather_data(lat, lon)
                
                if not weather_data:
                    await self.send_message(chat_id, "Error: No se pudieron obtener datos meteorológicos.")
                    return
                
                # Generar tablas para 4 días
                for day in range(4):
                    processed_data = self.process_weather_data(weather_data, day)
                    table = self.format_custom_location_table(day, lat, lon, processed_data)
                    await self.send_message(chat_id, f'<pre>{table}</pre>')
                    await asyncio.sleep(1)  # Pausa entre mensajes
            
            else:
                # Usar ubicaciones predefinidas
                logger.info("Procesando ubicaciones predefinidas")
                
                all_locations_data = {}
                
                # Obtener datos para cada ubicación
                for location_name, (lat, lon) in LOCATIONS.items():
                    weather_data = await self.fetch_weather_data(lat, lon)
                    if weather_data:
                        all_locations_data[location_name] = weather_data
                    await asyncio.sleep(1)  # Pausa entre ubicaciones
                
                if not all_locations_data:
                    await self.send_message(chat_id, "Error: No se pudieron obtener datos meteorológicos.")
                    return
                
                # Generar tablas para 4 días
                for day in range(4):
                    day_locations_data = {}
                    
                    for location_name, weather_data in all_locations_data.items():
                        processed_data = self.process_weather_data(weather_data, day)
                        day_locations_data[location_name] = processed_data
                    
                    table = self.format_table_for_day(day, day_locations_data)
                    await self.send_message(chat_id, f'<pre>{table}</pre>')
                    await asyncio.sleep(1)  # Pausa entre mensajes
        
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            if 'chat' in message:
                await self.send_message(
                    message['chat']['id'], 
                    "Error procesando su solicitud. Inténtelo más tarde."
                )
    
    async def get_updates(self):
        """Obtener updates de Telegram"""
        url = f'https://api.telegram.org/bot{BOT_TOKEN}/getUpdates'
        params = {'offset': self.last_update_id + 1, 'timeout': 30}
        
        try:
            async with self.session.get(url, params=params, timeout=35) as response:
                if response.status == 200:
                    data = await response.json()
                    if data['ok']:
                        return data['result']
                elif response.status == 409:
                    logger.error("Error obteniendo updates: 409")
                    await asyncio.sleep(5)
                else:
                    logger.error(f"Error obteniendo updates: {response.status}")
        except Exception as e:
            logger.error(f"Error obteniendo updates: {e}")
        
        return []
    
    async def run(self):
        """Ejecutar el bot"""
        await self.init_session()
        logger.info("Bot iniciado")
        
        while True:
            try:
                updates = await self.get_updates()
                
                for update in updates:
                    self.last_update_id = update['update_id']
                    
                    if 'message' in update:
                        await self.process_message(update['message'])
                
                if not updates:
                    await asyncio.sleep(1)
                    
            except Exception as e:
                logger.error(f"Error en el bucle principal: {e}")
                await asyncio.sleep(5)

# Servidor web para Render.com
async def health_check(request):
    return web.Response(text="OK", status=200)

async def init_web_app():
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    return app

async def main():
    # Crear instancia del bot
    bot = WeatherBot()
    
    # Crear aplicación web
    app = await init_web_app()
    
    # Iniciar servidor web en puerto de Render
    port = int(os.getenv('PORT', 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    logger.info(f"Servidor web iniciado en puerto {port}")
    
    # Ejecutar bot
    await bot.run()

if __name__ == '__main__':
    asyncio.run(main())

